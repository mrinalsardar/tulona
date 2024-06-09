import logging
import os
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

from tulona.exceptions import TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.task.helper import perform_comparison
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.sql import (
    get_information_schema_query,
    get_metric_query,
    get_query_output_as_df,
    get_table_fqn,
)

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    "compare_profiles": False,
}


@dataclass
class ProfileTask(BaseTask):
    profile: Dict
    project: Dict
    datasources: List[str]
    outfile_fqn: Union[Path, str]
    compare: bool = DEFAULT_VALUES["compare_profiles"]

    # Support for default values
    def __post_init__(self):
        for field in fields(self):
            # If there is a default and the value of the field is none we can assign a value
            if (
                not isinstance(field.default, _MISSING_TYPE)
                and getattr(self, field.name) is None
            ):
                setattr(self, field.name, field.default)

    def execute(self):

        log.info("------------------------ Starting task: profile")
        start_time = time.time()

        meta_frames = []
        table_constraint_frames = []
        metric_frames = []
        ds_name_compressed_list = []
        for ds_name in self.datasources:
            log.debug(f"Extracting configs for: {ds_name}")
            ds_name_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_name_compressed)

            ds_config = self.project["datasources"][ds_name]

            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]

            if "schema" not in ds_config or "table" not in ds_config:
                raise TulonaMissingPropertyError(
                    "Profiling requires `schema` and `table`"
                )

            # MySQL doesn't have logical database
            database = None
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            schema = ds_config["schema"]
            table = ds_config["table"]

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            log.info(f"Profiling {ds_name}")
            # Extract metadata
            log.debug("Extracting metadata")
            meta_query = get_information_schema_query(database, schema, table, "columns")
            log.debug(f"Executing query: {meta_query}")
            df_meta = get_query_output_as_df(
                connection_manager=conman, query_text=meta_query
            )
            df_meta = df_meta.rename(columns={c: c.lower() for c in df_meta.columns})
            meta_frames.append(df_meta)

            # Extract table constraints
            log.debug("Extracting table constraint info")
            table_constraint_query = get_information_schema_query(
                database, schema, table, "table_constraints"
            )
            log.debug(f"Executing query: {table_constraint_query}")
            df_tab_constraint = get_query_output_as_df(
                connection_manager=conman, query_text=table_constraint_query
            )
            df_tab_constraint = df_tab_constraint.rename(
                columns={c: c.lower() for c in df_tab_constraint.columns}
            )
            df_tab_constraint["table_constraint"] = (
                df_tab_constraint["table_name"]
                + "|"
                + df_tab_constraint["constraint_type"]
            )
            drop_cols = ["constraint_catalog", "constraint_schema", "constraint_name"]
            df_tab_constraint.drop(drop_cols, axis=1, inplace=True)
            table_constraint_frames.append(df_tab_constraint)

            # Extract metrics like min, max, avg, count, distinct count etc.
            log.debug("Extracting metrics")
            metrics = [
                "min",
                "max",
                "avg",
                "count",
                "distinct_count",
            ]
            data_container = (
                "(" + ds_config["query"] + ") t"
                if "query" in ds_config
                else get_table_fqn(database, schema, table)
            )
            metrics = list(map(lambda s: s.lower(), metrics))
            type_dict = df_meta[["column_name", "data_type"]].to_dict(orient="list")
            columns_dtype = {
                k: v for k, v in zip(type_dict["column_name"], type_dict["data_type"])
            }

            # TODO: quote for columns should be a config option, not an arbitrary thing
            try:
                log.debug("Trying query with unquoted column names")
                metric_query = get_metric_query(data_container, columns_dtype, metrics)
                log.debug(f"Executing query: {metric_query}")
                df_metric = get_query_output_as_df(
                    connection_manager=conman, query_text=metric_query
                )
            except Exception as exc:
                log.warning(f"Previous query failed with error: {exc}")
                log.debug("Trying query with quoted column names")
                metric_query = get_metric_query(
                    data_container,
                    columns_dtype,
                    metrics,
                    quoted=True,
                )
                log.debug(f"Executing query: {metric_query}")
                df_metric = get_query_output_as_df(
                    connection_manager=conman, query_text=metric_query
                )

            metric_dict = {m: [] for m in ["column_name"] + metrics}
            for col in df_meta["column_name"]:
                metric_dict["column_name"].append(col)
                for m in metrics:
                    try:
                        metric_value = df_metric.iloc[0][f"{col}_{m}"]
                    except Exception:
                        metric_value = df_metric.iloc[0][f"{col.lower()}_{m}"]
                    metric_dict[m].append(metric_value)
            df_metric = pd.DataFrame(metric_dict)

            metric_frames.append(df_metric)

        _ = create_dir_if_not_exist(Path(self.outfile_fqn).parent)
        if self.compare:
            # Metadata comparison
            log.debug("Preparing metadata comparison")
            df_meta_merge = perform_comparison(
                ds_compressed_names=ds_name_compressed_list,
                dataframes=meta_frames,
                on="column_name",
                case_insensitive=True,
            )
            log.debug(
                f"Calculated metadata comparison for {df_meta_merge.shape[0]} columns"
            )

            log.debug(f"Writing results into file: {self.outfile_fqn}")
            primary_key_col = df_meta_merge.pop("column_name")
            df_meta_merge.insert(loc=0, column="column_name", value=primary_key_col)
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                df_meta_merge.to_excel(
                    writer, sheet_name="Metadata Comparison", index=False
                )

            log.debug("Highlighting mismtach cells")
            highlight_mismatch_cells(
                excel_file=self.outfile_fqn,
                sheet="Metadata Comparison",
                num_ds=len(ds_name_compressed_list),
                skip_columns="column_name",
            )

            # Constraint comparison
            log.debug("Preparing table constraint comparison")
            df_tab_constr_merge = perform_comparison(
                ds_compressed_names=ds_name_compressed_list,
                dataframes=table_constraint_frames,
                on="table_constraint",
                how="outer",
                case_insensitive=True,
            )
            log.debug(
                "Calculated table constraint comparison for"
                f"{df_tab_constr_merge.shape[0]} columns"
            )

            log.debug(f"Writing results into file: {self.outfile_fqn}")
            primary_key_col = df_tab_constr_merge.pop("table_constraint")
            df_tab_constr_merge.insert(
                loc=0, column="table_constraint", value=primary_key_col
            )
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                df_tab_constr_merge.to_excel(
                    writer, sheet_name="Table Constraint Comparison", index=False
                )

            log.debug("Highlighting mismtach cells")
            highlight_mismatch_cells(
                excel_file=self.outfile_fqn,
                sheet="Table Constraint Comparison",
                num_ds=len(ds_name_compressed_list),
                skip_columns="table_constraint",
            )

            # Metric comparison
            log.debug("Preparing metric comparison")
            df_metric_merge = perform_comparison(
                ds_compressed_names=ds_name_compressed_list,
                dataframes=metric_frames,
                on="column_name",
                case_insensitive=True,
            )
            log.debug(
                f"Calculated metric comparison for {df_metric_merge.shape[0]} columns"
            )

            log.debug(f"Writing results into file: {self.outfile_fqn}")
            primary_key_col = df_metric_merge.pop("column_name")
            df_metric_merge.insert(loc=0, column="column_name", value=primary_key_col)
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                df_metric_merge.to_excel(
                    writer, sheet_name="Metric Comparison", index=False
                )

            log.debug("Highlighting mismtach cells")
            highlight_mismatch_cells(
                excel_file=self.outfile_fqn,
                sheet="Metric Comparison",
                num_ds=len(ds_name_compressed_list),
                skip_columns="column_name",
            )
        else:
            # Metadata output
            log.debug(f"Writing metadata into file: {self.outfile_fqn}")
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                for ds_name, df in zip(ds_name_compressed_list, meta_frames):
                    primary_key_col = df.pop("column_name")
                    df.insert(loc=0, column="column_name", value=primary_key_col)
                    df.to_excel(writer, sheet_name=f"{ds_name} Metadata", index=False)

            # Table constraint output
            log.debug(f"Writing table constraints into file: {self.outfile_fqn}")
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                for ds_name, df in zip(ds_name_compressed_list, table_constraint_frames):
                    primary_key_col = df.pop("table_constraint")
                    df.insert(loc=0, column="table_constraint", value=primary_key_col)
                    df.to_excel(
                        writer, sheet_name=f"{ds_name} Tab Constraint", index=False
                    )

            # Metric output
            log.debug(f"Writing metric into file: {self.outfile_fqn}")
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                for ds_name, df in zip(ds_name_compressed_list, metric_frames):
                    primary_key_col = df.pop("column_name")
                    df.insert(loc=0, column="column_name", value=primary_key_col)
                    df.to_excel(writer, sheet_name=f"{ds_name} Metric", index=False)

        exec_time = time.time() - start_time
        log.info(f"Finished task: profile in {exec_time:.2f} seconds")
