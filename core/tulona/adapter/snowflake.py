import logging
from typing import Dict

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

logging.getLogger("snowflake").setLevel(logging.ERROR)


def get_snowflake_engine(conn_profile: Dict):

    if "password" in conn_profile:
        engine = create_engine(
            URL(
                account=conn_profile["account"],
                warehouse=conn_profile["warehouse"],
                role=conn_profile["role"] if "role" in conn_profile else None,
                database=conn_profile["database"],
                schema=conn_profile["schema"],
                user=conn_profile["user"],
                password=conn_profile["password"],
            ),
            echo=False,
        )  # TODO: remove echo_pool="debug" param

    if "private_key" in conn_profile:

        # validate private_key
        if not conn_profile["private_key"].endswith(".p8"):
            raise ValueError(f"{conn_profile['private_key']} is not a valid private key")

        password = (
            conn_profile["private_key_passphrase"].encode()
            if "private_key_passphrase" in conn_profile
            else None
        )

        with open(conn_profile["private_key"], "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(), password=password, backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        engine = create_engine(
            URL(
                account=conn_profile["account"],
                warehouse=conn_profile["warehouse"],
                database=conn_profile["database"],
                schema=conn_profile["schema"],
                user=conn_profile["user"],
            ),
            connect_args={
                "private_key": pkb,
            },
            echo=False,
        )  # TODO: remove echo_pool="debug" param

    return engine
