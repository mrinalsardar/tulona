from typing import Dict

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from tulona.exceptions import TulonaMissingPropertyError


def get_mssql_engine(conn_profile: Dict):
    if "connection_string" in conn_profile:
        connection_string = conn_profile["connection_string"]
        url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(url)

    # validate properties
    if "connection_string" not in conn_profile:
        mandaory_properties = {"driver_version", "server", "database"}
        if len(mandaory_properties.intersection(set(conn_profile.keys()))) != len(
            mandaory_properties
        ):
            raise TulonaMissingPropertyError(
                f"One or more of {mandaory_properties} connection propertie[s] is/are missing"
            )

    return engine
