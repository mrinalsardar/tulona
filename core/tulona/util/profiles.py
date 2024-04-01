from pathlib import Path
from typing import Dict


def profile_path() -> str:
    return Path(Path.home(), ".tulona", "profiles.yml")


def profile_exists():
    return profile_path().exists()


def extract_profile_name(project: Dict, datasource: str):
    ds_profile_name = project["datasources"][datasource]["connection_profile"]
    return ds_profile_name


def get_connection_profile(profile: Dict, project: Dict, datasource: str):
    ds_profile_name = extract_profile_name(project, datasource)
    connection_profile = profile["profiles"][ds_profile_name]
    return connection_profile
