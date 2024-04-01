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
from tulona.util.sql import get_query_output_as_df

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

    def get_column_info(self, conman, database, schema, table):
        if database:
            query = f"""
            select * from information_schema.columns
            where table_catalog = '{database}'
            and table_schema = '{schema}'
            and table_name = '{table}'
            """
        else:
            query = f"""
            select * from information_schema.columns
            where table_schema = '{schema}'
            and table_name = '{table}'
            """
        log.debug(f"Executing query: {query}")
        df = get_query_output_as_df(connection_manager=conman, query_text=query)

        return df

    def get_outfile_fqn(self, ds_list):
        outdir = create_dir_if_not_exist(self.project["outdir"])
        out_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
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

            df = self.get_column_info(conman, database, schema, table)
            df = df.rename(columns={c: c.lower() for c in df.columns})
            df_collection.append(df)

        outfile_fqn = self.get_outfile_fqn(ds_name_compressed_list)

        if self.compare:
            log.debug("Preparing metadata comparison")
            common_columns = set(df_collection[0].columns.tolist())
            df_collection_final = []
            for ds_name, df in zip(ds_name_compressed_list, df_collection):
                common_columns = common_columns.intersection(set(df.columns.tolist()))

            print(f"num common cols: {len(common_columns)}")

            for ds_name, df in zip(ds_name_compressed_list, df_collection):
                df = df[list(common_columns)]
                df = df.rename(
                    columns={
                        c: f"{c}_{ds_name}" if c != "column_name" else c
                        for c in df.columns
                    }
                )
                df_collection_final.append(df)

            df_merge = df_collection_final.pop()
            for df in df_collection_final:
                df_merge = pd.merge(
                    left=df_merge, right=df, on="column_name", how="inner"
                )
            df_merge = df_merge[sorted(df_merge.columns.tolist())]

            log.debug(f"Writing results into file: {outfile_fqn}")
            df_merge.to_excel(outfile_fqn, sheet_name="Metadata Comparison", index=False)
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
                    df.to_excel(writer, sheet_name=f"{ds_name} Metadata", index=False)

        end_time = time.time()
        log.info("Finished task: profiling")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
