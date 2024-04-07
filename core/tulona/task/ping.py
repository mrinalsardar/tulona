import logging
import time
from dataclasses import dataclass
from typing import Dict, List

from tulona.task.base import BaseTask
from tulona.util.profiles import get_connection_profile

log = logging.getLogger(__name__)


@dataclass
class PingTask(BaseTask):
    profile: Dict
    project: Dict
    datasources: List[str]

    def execute(self):

        log.info("Starting task: Ping")
        start_time = time.time()

        for ds in self.datasources:
            log.debug(f"Testing connection to data source: {ds}")

            connection_profile = get_connection_profile(self.profile, self.project, ds)
            try:
                conman = self.get_connection_manager(conn_profile=connection_profile)
                with conman.engine.connect() as connection:
                    results = connection.execute(
                        "select * from information_schema.tables"
                    ).fetchone()
                    _ = results[0]
                    log.info("Connection successful")
            except Exception as exp:
                log.error(f"Connection to data source {ds} failed because of: {exp}")

        end_time = time.time()
        log.info("Finished task: Ping")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
