import logging
from dataclasses import dataclass

from tulona.adapter.base.connection import BaseConnectionManager
from tulona.adapter.mssql import get_mssql_engine
from tulona.adapter.mysql import get_mysql_engine
from tulona.adapter.postgres import get_postgres_engine
from tulona.adapter.snowflake import SnowflakeConnection
from tulona.exceptions import TulonaNotImplementedError

log = logging.getLogger(__name__)


@dataclass
class ConnectionManager(BaseConnectionManager):
    def get_engine(self):
        dbtype = self.conn_profile["type"].lower()
        if dbtype == "snowflake":
            self.engine = SnowflakeConnection(credentials=self.conn_profile).get_engine()
        elif dbtype == "mssql":
            self.engine = get_mssql_engine(self.conn_profile)
        elif dbtype == "postgres":
            self.engine = get_postgres_engine(self.conn_profile)
        elif dbtype == "mysql":
            self.engine = get_mysql_engine(self.conn_profile)
        else:
            raise TulonaNotImplementedError(
                f"Tulona connection manager is not set up for {dbtype}"
            )

    def open(self):
        self.get_engine()
        self.conn = self.engine.connect()

    def close(self):
        self.conn.close()
        self.engine.dispose()
