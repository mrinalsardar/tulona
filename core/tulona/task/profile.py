import logging
import os
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

from tulona.exceptions import TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.util.sql import (
    get_metadata_query,
    get_metric_query,
    get_query_output_as_df,
    get_table_fqn,
)
from tulona.task.helper import perform_comparison
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile

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

        df_collection = []
        ds_name_compressed_list = []
        for ds_name in self.datasources:
            log.debug(f"Extracting configs for: {ds_name}")
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
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
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = None
            schema = ds_config["schema"]
            table = ds_config["table"]

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            log.info(f"Profiling {ds_name}")
            metrics = [
                "min",
                "max",
                "avg",
                "count",
                "distinct_count",
            ]

            # Extract metadata
            log.debug("Extracting metadata")
            meta_query = get_metadata_query(database, schema, table)
            log.debug(f"Executing query: {meta_query}")
            df_meta = get_query_output_as_df(connection_manager=conman, query_text=meta_query)
            df_meta = df_meta.rename(columns={c: c.lower() for c in df_meta.columns})

            # Extract metrics like min, max, avg, count, distinct count etc.
            log.debug("Extracting metrics")
            data_container = ("(" + ds_config["query"] + ") t" if "query" in ds_config else get_table_fqn(database, schema, table))
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

            # Combine meta and metric data
            df = pd.merge(left=df_meta, right=df_metric, how="inner", on="column_name")
            df_collection.append(df)

        _ = create_dir_if_not_exist(self.outfile_fqn.parent)
        if self.compare:
            log.debug("Preparing metadata comparison")
            df_merge = perform_comparison(
                ds_name_compressed_list,
                df_collection,
                "column_name",
                case_insensitive=True,
            )
            log.debug(f"Calculated comparison for {df_merge.shape[0]} columns")

            log.debug(f"Writing results into file: {self.outfile_fqn}")
            primary_key_col = df_merge.pop("column_name")
            df_merge.insert(loc=0, column="column_name", value=primary_key_col)
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                df_merge.to_excel(writer, sheet_name="Metadata Comparison", index=False)

            log.debug("Highlighting mismtach cells")
            highlight_mismatch_cells(
                excel_file=self.outfile_fqn,
                sheet="Metadata Comparison",
                num_ds=len(ds_name_compressed_list),
                skip_columns="column_name",
            )
        else:
            log.debug(f"Writing results into file: {self.outfile_fqn}")
            with pd.ExcelWriter(self.outfile_fqn) as writer:
                for ds_name, df in zip(ds_name_compressed_list, df_collection):
                    primary_key_col = df.pop("column_name")
                    df.insert(loc=0, column="column_name", value=primary_key_col)
                    df.to_excel(writer, sheet_name=f"{ds_name} Metadata", index=False)

        exec_time = time.time() - start_time
        log.info(f"Finished task: profile in {exec_time:.2f} seconds")
