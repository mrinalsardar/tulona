import logging
from typing import Any, Dict
from pathlib import Path
from itertools import chain, combinations
from dataclasses import dataclass
from tulona.util.filesystem import path_exists
from tulona.util.yaml_parser import read_yaml
from tulona.exceptions import (
    TulonaInvalidProjectConfigError,
    TulonaProjectException
)

log = logging.getLogger(__name__)

PROJECT_FILE_NAME = 'tulona_project.yml'


class Project:
    @property
    def get_project_root(self):
        return Path().absolute()

    @property
    def project_conf_path(self) -> str:
        return Path(self.get_project_root, PROJECT_FILE_NAME)


    def validate_project_config(self, project_dict_raw: dict) -> bool:
        # TODO: implement validations for project config
        valid = True

        if not valid:
            raise TulonaInvalidProjectConfigError(
                "Project config is not valid"
            )


    def load_project_config(self) -> None:
        project_file_uri = self.project_conf_path
        log.debug(f"Attempting to load project config from {project_file_uri}")

        if not path_exists(project_file_uri):
            raise TulonaProjectException(f"Project file {project_file_uri} does not exist.")

        project_dict_raw = read_yaml(project_file_uri)

        if not isinstance(project_dict_raw, dict):
            raise (f"{project_file_uri} could not be parsed to a python dictionary.")

        log.debug(f"Project config is successfully loaded from {project_file_uri}")

        self.validate_project_config(project_dict_raw)

        self.project_config_raw = project_dict_raw


    def get_eligible_connection_list(self, connection_profiles: dict, databases: dict) -> list[str]:
        available_connections = list(connection_profiles.keys())
        # log.debug(f"Available connection profiles: {available_connections}")

        available_databases = list(chain.from_iterable([list(dc.keys()) for dc in databases]))
        # log.debug(f"Databases available for comparison: {available_databases}")

        eligible_connections = list(set(available_connections).intersection(available_databases))
        # log.debug(f"Eligible connections/databases for comparison: {eligible_connections}")

        return eligible_connections


    def get_eligible_connection_profiles(self) -> list[list[dict]]:
        connection_profiles = self.project_config_raw['connection_profiles']
        databases = self.project_config_raw['databases']

        eligible_connections = self.get_eligible_connection_list(
            connection_profiles=connection_profiles,
            databases=databases
        )

        eligible_profiles = []
        for candidates in databases:
            # Checking if current candidates are eligible
            available_set = set(candidates.keys()).intersection(set(eligible_connections))
            if len(candidates) == len(available_set):
                combo = []
                for p in candidates:
                    profile = connection_profiles[p]
                    profile['database'] = candidates[p]

                    combo.append(profile)

                eligible_profiles.append(combo)

        return eligible_profiles