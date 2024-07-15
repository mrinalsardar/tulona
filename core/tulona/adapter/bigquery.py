from typing import Dict

from sqlalchemy import create_engine


class BigQueryConnection:
    _instance = None

    def __new__(cls, credentials: Dict):
        if cls._instance is None:
            cls._instance = super(BigQueryConnection, cls).__new__(cls)
            cls._instance._init_connection(credentials)
        return cls._instance

    def _init_connection(self, credentials: Dict):
        if credentials["method"] == "service_account":
            # validate private_key
            if not credentials["key_file"].endswith(".json"):
                raise ValueError(f"{credentials['key_file']} is not valid.")

            self.engine = create_engine(
                url=f"bigquery://{credentials['project']}",
                credentials_path=credentials["key_file"],
                echo=False,
            )

    def get_engine(self):
        return self.engine
