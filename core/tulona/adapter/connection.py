import logging
from typing import Dict
from dataclasses import dataclass
from tulona.adapter.base.connection import BaseConnectionManager
from tulona.adapter.snowflake import get_snowflake_engine
from tulona.adapter.mssql import get_mssql_engine

log = logging.getLogger(__name__)

@dataclass
class ConnectionManager(BaseConnectionManager):
    def get_engine(self):
        if self.conn_profile['type'].lower() == 'snowflake':
            self.engine = get_snowflake_engine(self.conn_profile)
        if self.conn_profile['type'].lower() == 'mssql':
            self.engine = get_mssql_engine(self.conn_profile)

    def open(self):
        self.get_engine()
        # self.conn = self.engine.open()
        self.conn = self.engine.connect()

    def close(self):
        self.conn.close()
