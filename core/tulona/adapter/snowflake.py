import logging
from typing import Dict

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

logging.getLogger("snowflake").setLevel(logging.ERROR)


class SnowflakeConnection:
    _instance = None

    def __new__(cls, credentials: Dict):
        if cls._instance is None:
            cls._instance = super(SnowflakeConnection, cls).__new__(cls)
            cls._instance._init_connection(credentials)
        return cls._instance

    def _init_connection(self, credentials: Dict):
        if "password" in credentials:
            self.engine = create_engine(
                URL(
                    account=credentials["account"],
                    warehouse=credentials["warehouse"],
                    role=credentials["role"] if "role" in credentials else None,
                    database=credentials["database"],
                    schema=credentials["schema"],
                    user=credentials["user"],
                    password=credentials["password"],
                ),
                echo=False,
            )  # TODO: remove echo_pool="debug" param

        if "private_key" in credentials:
            # validate private_key
            if not credentials["private_key"].endswith(".p8"):
                raise ValueError(
                    f"{credentials['private_key']} is not a valid private key"
                )

            password = (
                credentials["private_key_passphrase"].encode()
                if "private_key_passphrase" in credentials
                else None
            )

            with open(credentials["private_key"], "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(), password=password, backend=default_backend()
                )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            self.engine = create_engine(
                URL(
                    account=credentials["account"],
                    warehouse=credentials["warehouse"],
                    database=credentials["database"],
                    schema=credentials["schema"],
                    user=credentials["user"],
                ),
                connect_args={
                    "private_key": pkb,
                },
                echo=False,
            )  # TODO: remove echo_pool="debug" param

        if "authenticator" in credentials:
            if credentials["authenticator"] == "externalbrowser":
                self.engine = create_engine(
                    URL(
                        account=credentials["account"],
                        warehouse=credentials["warehouse"],
                        role=credentials["role"] if "role" in credentials else None,
                        database=credentials["database"],
                        schema=credentials["schema"],
                        user=credentials["user"],
                        authenticator=credentials["authenticator"],
                    ),
                    echo=False,
                )  # TODO: remove echo_pool="debug" param

    def get_engine(self):
        return self.engine
