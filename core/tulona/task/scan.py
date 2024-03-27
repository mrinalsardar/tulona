import time
import logging
from dataclasses import dataclass
from tulona.task.base import BaseTask
from tulona.config.runtime import RunConfig
from typing import Dict, List
from tulona.util.profiles import get_connection_profile

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

        # TODO: Change the implementation
        for ds in self.datasources:
            log.debug(f"Testing connection to data source: {ds}")

            connection_profile = get_connection_profile(self.profile, self.project, ds)
            try:
                conman = self.get_connection_manager(conn_profile=connection_profile)
                with conman.engine.open() as connection:
                    results = connection.execute('select * from information_schema').fetchone()
            except Exception as exp:
                log.error(f"Connection to data source {ds} failed because of: {exp}")

        end_time = time.time()

        end_time = time.time()
        log.info("Finished task: Scan")
        log.info(f"Total time taken: {end_time - start_time} seconds")
