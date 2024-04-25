import logging
import time
import traceback
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

        log.info("Starting task: ping")
        start_time = time.time()

        log.debug(f"Data sources to ping: {self.datasources}")

        for ds in self.datasources:
            connection_profile = get_connection_profile(
                self.profile, self.project["datasources"][ds]
            )
            log.info(
                f"Testing connection to data source: {ds}[{connection_profile['type']}]"
            )
            try:
                conman = self.get_connection_manager(conn_profile=connection_profile)
                with conman.engine.connect() as connection:
                    results = connection.execute(
                        "select * from information_schema.tables"
                    ).fetchone()
                    _ = results[0]
                    log.info("Connection successful")
            except Exception:
                log.error(f"Connection failed with error: {traceback.format_exc()}")

        end_time = time.time()
        log.info("Finished task: ping")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
