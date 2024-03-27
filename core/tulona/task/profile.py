import time
import logging
from dataclasses import dataclass
from tulona.task.base import BaseTask
from tulona.config.runtime import RunConfig
from typing import Dict, List

log = logging.getLogger(__name__)

@dataclass
class ProfileTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]

    def execute(self):

        log.info("Starting task: profiling")
        start_time = time.time()

        # TODO: Change the implementation
        for ds in self.datasources:
            log.debug(f"Testing connection to data source: {ds}")

            all_datasources = self.project['datasources']
            ds_profile_name = (
                [dc for dc in all_datasources if dc['name'] == ds][0]['connection_profile']
            )
            connection_profile = self.profile[self.project['name']]['profiles'][ds_profile_name]

            try:
                conman = self.get_connection_manager(conn_profile=connection_profile)
                with conman.engine.open() as connection:
                    results = connection.execute('select * from information_schema').fetchone()
            except Exception as exp:
                log.error(f"Connection to data source {ds} failed because of: {exp}")

        end_time = time.time()

        end_time = time.time()
        log.info("Finished task: profiling")
        log.info(f"Total time taken: {end_time - start_time} seconds")
