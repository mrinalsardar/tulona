import logging
import dask.dataframe as dd
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
from tulona.task.base import BaseTask
from tulona.adapter.connection import ConnectionManager
from tulona.util.database import (
    get_schemas_from_db,
    get_table_primary_keys,
    get_tables_from_schema,
)
from tulona.exceptions import TulonaUnSupportedExecEngine
from tulona.util.filesystem import get_result_dir
from tulona.config.runtime import RunConfig
from typing import Dict, Union, Tuple


log = logging.getLogger(__name__)


RESTULT_LOCATIONS = {
    "result_dir": "results",
    "metadiff_dir": "metadata",
    "result_meta_outfile": "result_metadata.csv",
    "datadiff_dir": "datadiff",
}


@dataclass
class CompareTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig

    def get_connection(
        self,
        dbtype: str,
        host: str,
        port: Union[str, int],
        username: str,
        password: str,
        database: str,
    ) -> ConnectionManager:
        conman = ConnectionManager(
            dbtype=dbtype,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        conman.open()

        return conman

    def execute(self):
        log.info("Starting comparison")
        log.info("To be implemented")
