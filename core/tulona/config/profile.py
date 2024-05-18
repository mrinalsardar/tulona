import logging
from pathlib import Path
from typing import Dict, Union

from tulona.exceptions import TulonaMissingPropertyError, TulonaProfileException
from tulona.util.filesystem import path_exists
from tulona.util.yaml_parser import read_yaml

log = logging.getLogger(__name__)

PROFILE_FOLDER_NAME = ".tulona"
PROFILE_FILE_NAME = "profiles.yml"


class Profile:
    @property
    def get_profile_root(self):
        return Path(Path.home(), ".tulona")

    @property
    def profile_conf_path(self) -> Union[str, Path]:
        return Path(self.get_profile_root, PROFILE_FILE_NAME)

    def validate_profile_config(self, profile_dict_raw: dict) -> None:
        for proj in profile_dict_raw:
            proj_dict = profile_dict_raw[proj]
            if "profiles" not in proj_dict:
                raise TulonaMissingPropertyError(
                    f"Project {proj} doesn't have any connection profiles defined"
                )

            for prof in proj_dict["profiles"]:
                prof_dict = proj_dict["profiles"][prof]
                if "type" not in prof_dict:
                    raise TulonaMissingPropertyError(
                        f"Connection profile {prof} doesn't 'type' specified"
                    )

    def load_profile_config(self) -> Dict:
        profile_file_uri = self.profile_conf_path
        log.debug(f"Attempting to load profile config from {profile_file_uri}")

        if not path_exists(profile_file_uri):
            raise TulonaProfileException(
                f"Profile file {profile_file_uri} does not exist."
            )

        profile_dict_raw = read_yaml(str(profile_file_uri))

        if not isinstance(profile_dict_raw, dict):
            raise TulonaProfileException(
                f"{profile_file_uri} could not be parsed to a python dictionary."
            )

        log.debug(f"Profile config is successfully loaded from {profile_file_uri}")

        self.validate_profile_config(profile_dict_raw)

        return profile_dict_raw
