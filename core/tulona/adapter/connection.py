import logging
from sqlalchemy import URL, create_engine
from tulona.adapter.base.connection import BaseConnectionManager


log = logging.getLogger(__name__)


def adapter_type(name):
    return {
        "postgres": "postgresql",
        "mysql": "mysql+pymysql",
    }[name]


class ConnectionManager(BaseConnectionManager):
    def connection_string(self):
        return URL.create(
            adapter_type(self.dbtype),
            username=self.username,
            password=self.password,  # plain (unescaped) text
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def get_engine(self):
        self.engine = create_engine(
            self.connection_string(),
            echo=False
        )  # TODO: remove echo_pool="debug" param

    def open(self):
        self.get_engine()
        self.conn = self.engine.connect()

    def close(self):
        self.conn.close()
