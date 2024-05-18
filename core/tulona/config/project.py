import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import BaseModel

from tulona.exceptions import TulonaInvalidProjectConfigError, TulonaProjectException
from tulona.util.filesystem import path_exists
from tulona.util.yaml_parser import read_yaml

log = logging.getLogger(__name__)

PROJECT_FILE_NAME = "tulona-project.yml"


# TODO: Add datasource model to validation
class ProjectModel(BaseModel):
    version: str
    name: str
    config_version: int = 1
    engine: Optional[str] = "pandas"
    outdir: str = "output"
    datasources: Dict
    task_config: Optional[List[Dict]] = []


class Project:
    @property
    def get_project_root(self):
        return Path().absolute()

    @property
    def project_conf_path(self) -> Union[Path, str]:
        return Path(self.get_project_root, PROJECT_FILE_NAME)

    def validate_project_config(self, project_dict_raw: Dict) -> Dict:
        try:
            return ProjectModel(**project_dict_raw).model_dump()
        except TulonaInvalidProjectConfigError as exc:
            raise TulonaInvalidProjectConfigError(exc.formatted_message)

    def load_project_config(self) -> Dict:
        project_file_uri = self.project_conf_path
        log.debug(f"Attempting to load project config from {project_file_uri}")

        if not path_exists(project_file_uri):
            raise TulonaProjectException(
                f"Project file {project_file_uri} does not exist."
            )

        project_dict_raw = read_yaml(str(project_file_uri))

        if not isinstance(project_dict_raw, dict):
            raise TulonaProjectException(
                f"{project_file_uri} could not be parsed to a python dictionary."
            )

        log.debug(f"Project config is successfully loaded from {project_file_uri}")

        final_project_dict = self.validate_project_config(project_dict_raw)

        return final_project_dict
