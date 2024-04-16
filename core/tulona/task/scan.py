import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Union

from tulona.config.runtime import RunConfig
from tulona.task.base import BaseTask
from tulona.util.excel import dataframes_into_excel
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.sql import get_query_output_as_df

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    "compare_scans": False,
}
META_EXCLUSION = {
    "schemas": ["INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA"],
}


@dataclass
class ScanTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]
    compare: bool = DEFAULT_VALUES["compare_scans"]

    def execute(self):
        log.info("Starting task: scan")
        log.debug(f"Datasource: {self.datasources}")
        log.debug(f"Compare: {self.compare}")
        log.debug(f"Output file: {self.outfile_fqn}")
        start_time = time.time()

        scan_result = {}
        write_map = {}
        ds_name_compressed_list = []
        for ds_name in self.datasources:
            log.info(f"Processing datasource {ds_name}")
            ds_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_compressed)
            ds_config = self.project["datasources"][ds_name]

            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]
            log.debug(f"Database type: {dbtype}")

            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            # MySQL doesn't have logical database
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = "def"

            # Database scan
            log.debug(f"Performing database scan for: {database}")
            if "schema" in ds_config:
                schemata_query = f"""
                select
                    *
                from
                    information_schema.schemata
                where
                    upper(catalog_name) = '{database.upper()}'
                    and upper(schema_name) = '{ds_config["schema"].upper()}'
                """
            else:
                schemata_query = f"""
                select
                    *
                from
                    information_schema.schemata
                where
                    upper(catalog_name) = '{database.upper()}'
                    and upper(schema_name) not in (
                        {"', '".join(META_EXCLUSION['schemas'])}
                    )
                """
            log.debug(f"Executing query: {schemata_query}")
            schemata_df = get_query_output_as_df(
                connection_manager=conman, query_text=schemata_query
            )
            log.debug(f"Number of schemas found: {schemata_df.shape[0]}")

            schemata_df = schemata_df.rename(
                columns={c: c.lower() for c in schemata_df.columns}
            )
            write_map[ds_compressed] = schemata_df

            # Schema scan
            schema_list = schemata_df["schema_name"].tolist()
            for schema in schema_list:
                log.debug(f"Performing schema scan for: {schema}")
                tables_query = f"""
                select
                    *
                from
                    information_schema.tables
                where
                    upper(table_schema) = '{schema.upper()}'
                """
                log.debug(f"Executing query: {tables_query}")
                tables_df = get_query_output_as_df(
                    connection_manager=conman, query_text=tables_query
                )
                log.debug(f"Number of tables found: {tables_df.shape[0]}")
                tables_df = tables_df.rename(
                    columns={c: c.lower() for c in tables_df.columns}
                )
                scan_result[ds_name] = {schema: tables_df}
                write_map[f"{ds_compressed}_{schema}"] = tables_df

        # Writing scan result
        log.debug(f"Writing scan result into: {self.outfile_fqn}")
        _ = create_dir_if_not_exist(self.project["outdir"])
        dataframes_into_excel(
            sheet_df_map=write_map,
            outfile_fqn=self.outfile_fqn,
            mode="a" if os.path.exists(self.outfile_fqn) else "w",
        )

        # # Compare database extracts
        # if self.compare:
        #     log.debug("Preparing metadata comparison")
        #     df_merge = perform_comparison(
        #         ds_name_compressed_list, df_collection, "column_name"
        #     )

        end_time = time.time()
        log.info("Finished task: scan")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
