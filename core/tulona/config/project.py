import logging
from pathlib import Path
from tulona.util.filesystem import path_exists
from tulona.util.yaml_parser import read_yaml
from tulona.exceptions import (
    TulonaInvalidProjectConfigError,
    TulonaProjectException
)

log = logging.getLogger(__name__)

PROJECT_FILE_NAME = "tulona-project.yml"


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
            raise TulonaInvalidProjectConfigError("Project config is not valid")

    def load_project_config(self) -> None:
        project_file_uri = self.project_conf_path
        log.debug(f"Attempting to load project config from {project_file_uri}")

        if not path_exists(project_file_uri):
            raise TulonaProjectException(f"Project file {project_file_uri} does not exist.")

        project_dict_raw = read_yaml(project_file_uri)

        if not isinstance(project_dict_raw, dict):
            raise TulonaProjectException(
                f"{project_file_uri} could not be parsed to a python dictionary."
            )

        log.debug(f"Project config is successfully loaded from {project_file_uri}")

        self.validate_project_config(project_dict_raw)

        return project_dict_raw