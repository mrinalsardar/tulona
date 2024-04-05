import logging
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.task.base import BaseTask
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.sql import get_metadata_query, get_metric_query, get_query_output_as_df

log = logging.getLogger(__name__)


@dataclass
class ProfileTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    compare: bool = False

    # Support for default values
    def __post_init__(self):
        for field in fields(self):
            # If there is a default and the value of the field is none we can assign a value
            if (
                not isinstance(field.default, _MISSING_TYPE)
                and getattr(self, field.name) is None
            ):
                setattr(self, field.name, field.default)

    def get_outfile_fqn(self, ds_list):
        outdir = create_dir_if_not_exist(self.project["outdir"])
        out_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        outfile = f"{'_'.join(ds_list)}_profiles_{out_timestamp}.xlsx"
        outfile_fqn = Path(outdir, outfile)
        return outfile_fqn

    def execute(self):

        log.info("Starting task: profiling")
        start_time = time.time()

        df_collection = []
        ds_name_compressed_list = []
        for ds_name in self.datasources:
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
            ds_name_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_name_compressed)
            log.debug(f"Extracting metadata for {ds_name}")

            ds_config = self.project["datasources"][ds_name]
            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]

            # MySQL doesn't have logical database
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = None
            schema = ds_config["schema"]
            table = ds_config["table"]

            connection_profile = get_connection_profile(
                self.profile, self.project, ds_name
            )
            conman = self.get_connection_manager(conn_profile=connection_profile)

            # Extract metadata
            log.debug("Extracting metadata")
            meta_query = get_metadata_query(database, schema, table)
            log.debug(f"Executing query: {meta_query}")
            df_meta = get_query_output_as_df(
                connection_manager=conman, query_text=meta_query
            )
            df_meta = df_meta.rename(columns={c: c.lower() for c in df_meta.columns})

            # Extract metrics like min, max, avg, count, distinct count etc.
            log.debug("Extracting metrics")
            metrics = [
                "min",
                "max",
                "avg",
                "count",
                "distinct_count",
            ]
            # metrics = ["count", "distinct_count"]
            metrics = list(map(lambda s: s.lower(), metrics))
            type_dict = df_meta[["column_name", "data_type"]].to_dict(orient="list")
            columns_dtype = {
                k: v for k, v in zip(type_dict["column_name"], type_dict["data_type"])
            }

            try:
                log.debug("Trying query with unquoted column names")
                metric_query = get_metric_query(
                    database, schema, table, columns_dtype, metrics
                )
                log.debug(f"Executing query: {metric_query}")
                df_metric = get_query_output_as_df(
                    connection_manager=conman, query_text=metric_query
                )
            except Exception as exp:
                log.warn(f"Previous query failed with error: {exp}")
                log.debug("Trying query with quoted column names")
                metric_query = get_metric_query(
                    database,
                    schema,
                    table,
                    columns_dtype,
                    metrics,
                    quoted=True,
                )
                log.debug(f"Executing query: {metric_query}")
                df_metric = get_query_output_as_df(
                    connection_manager=conman, query_text=metric_query
                )

            log.debug("Converting metric data into presentable format")
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

        outfile_fqn = self.get_outfile_fqn(ds_name_compressed_list)

        if self.compare:
            log.debug("Preparing metadata comparison")
            common_columns = {c.lower() for c in df_collection[0].columns.tolist()}
            df_collection_final = []
            for ds_name, df in zip(ds_name_compressed_list, df_collection):
                colset = {c.lower() for c in df.columns.tolist()}
                common_columns = common_columns.intersection(colset)

            for ds_name, df in zip(ds_name_compressed_list, df_collection):
                df = df[list(common_columns)]
                df = df.rename(
                    columns={
                        c: f"{c}_{ds_name}" if c.lower() != "column_name" else c.lower()
                        for c in df.columns
                    }
                )
                df["column_name"] = df["column_name"].str.lower()
                df_collection_final.append(df)

            df_merge = df_collection_final.pop()
            for df in df_collection_final:
                df_merge = pd.merge(
                    left=df_merge, right=df, on="column_name", how="inner"
                )
            df_merge = df_merge[sorted(df_merge.columns.tolist())]
            log.debug(f"Calculated comparison for {df_merge.shape[0]} columns")

            log.debug(f"Writing results into file: {outfile_fqn}")
            primary_key_col = df_merge.pop("column_name")
            df_merge.insert(loc=0, column="column_name", value=primary_key_col)
            df_merge.to_excel(outfile_fqn, sheet_name="Metadata Comparison", index=False)

            log.debug("Highlighting mismtach cells")
            highlight_mismatch_cells(
                excel_file=outfile_fqn,
                sheet="Metadata Comparison",
                num_ds=len(ds_name_compressed_list),
                skip_columns="column_name",
            )
        else:
            log.debug(f"Writing results into file: {outfile_fqn}")
            with pd.ExcelWriter(outfile_fqn) as writer:
                for ds_name, df in zip(ds_name_compressed_list, df_collection):
                    primary_key_col = df.pop("column_name")
                    df.insert(loc=0, column="column_name", value=primary_key_col)
                    df.to_excel(writer, sheet_name=f"{ds_name} Metadata", index=False)

        end_time = time.time()
        log.info("Finished task: profiling")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
