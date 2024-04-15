import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Union

from tulona.config.runtime import RunConfig
from tulona.task.base import BaseTask
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.sql import get_query_output_as_df

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    "compare_scans": False,
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
        start_time = time.time()

        for ds_name in self.datasources:
            log.info(f"Processing datasource {ds_name}")
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

            schemata_query = f"""
            select * from information_schema.schemata
            where upper(catalog_name) = '{database.upper()}'
            and upper(schema_name) <> 'INFORMATION_SCHEMA'
            """
            log.debug(f"Executing query: {schemata_query}")
            schemata_df = get_query_output_as_df(
                connection_manager=conman, query_text=schemata_query
            )
            log.debug(f"Number of schemas found: {schemata_df.shape[0]}")

            print(schemata_df.head())
            # TODO: Extract tables from schemas and implement comparison

        end_time = time.time()
        log.info("Finished task: scan")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
