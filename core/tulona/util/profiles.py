from pathlib import Path
from typing import Dict


def profile_path() -> str:  # pragma: no cover
    return Path(Path.home(), ".tulona", "profiles.yml")


def profile_exists():  # pragma: no cover
    return profile_path().exists()


def extract_profile_name(project: Dict, datasource: str):
    ds_profile_name = project["datasources"][datasource]["connection_profile"]
    return ds_profile_name


def get_connection_profile(profile: Dict, config: Dict):
    ds_profile_name = config["connection_profile"]
    connection_profile = profile["profiles"][ds_profile_name]
    return connection_profile
