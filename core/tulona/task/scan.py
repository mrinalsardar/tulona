import logging
import time
from dataclasses import dataclass
from typing import Dict, List

from tulona.config.runtime import RunConfig
from tulona.task.base import BaseTask
from tulona.util.profiles import extract_profile_name, get_connection_profile

log = logging.getLogger(__name__)


@dataclass
class ScanTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]

    def execute(self):
        log.info("Starting task: Scan")
        start_time = time.time()

        for ds_name in self.datasources:
            ds_config = self.project["datasources"][ds_name]

            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]
            log.debug(f"Database type: {dbtype}")

            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

        end_time = time.time()
        log.info("Finished task: Scan")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
