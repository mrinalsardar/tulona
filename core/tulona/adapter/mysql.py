from typing import Dict

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from tulona.exceptions import TulonaMissingPropertyError


def get_mysql_engine(conn_profile: Dict):
    # TODO: Implement
    # if 'connection_string' in conn_profile:
    #     connection_string = conn_profile['connection_string']
    #     url = URL.create(drivername="mysql+pymysql", query={"odbc_connect": connection_string})
    #     engine = create_engine(url)

    # validate properties
    if "connection_string" not in conn_profile:
        mandaory_properties = {"host", "database", "username"}
        if len(mandaory_properties.intersection(set(conn_profile.keys()))) != len(
            mandaory_properties
        ):
            raise TulonaMissingPropertyError(
                f"One or more of {mandaory_properties} connection propertie[s] is/are missing"
            )

    if "password" in conn_profile:
        url = URL.create(
            drivername="mysql+pymysql",
            username=conn_profile["username"],
            password=conn_profile["password"],  # plain (unescaped) text
            database=conn_profile["database"],
            host=conn_profile["host"],
            port=conn_profile["port"],
        )
        engine = create_engine(url, echo=False)

    return engine
