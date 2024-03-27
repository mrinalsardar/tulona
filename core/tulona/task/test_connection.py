import time
import logging
from dataclasses import dataclass
from tulona.task.base import BaseTask
from typing import Dict, List
from tulona.util.profiles import get_connection_profile

log = logging.getLogger(__name__)


@dataclass
class TestConnectionTask(BaseTask):
    profile: Dict
    project: Dict
    datasources: List[str]

    def execute(self):

        log.info("Starting task: Test Connection")
        start_time = time.time()

        for ds in self.datasources:
            log.debug(f"Testing connection to data source: {ds}")

            connection_profile = get_connection_profile(self.profile, self.project, ds)
            try:
                conman = self.get_connection_manager(conn_profile=connection_profile)
                with conman.engine.open() as connection:
                    results = connection.execute('select * from information_schema').fetchone()
                    _ = results[0]
                    log.info("Connection successful")
            except Exception as exp:
                log.error(f"Connection to data source {ds} failed because of: {exp}")

        end_time = time.time()
        log.info("Finished task: Test Connection")
        log.info(f"Total time taken: {end_time - start_time} seconds")
