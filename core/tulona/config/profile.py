import logging
from pathlib import Path

from tulona.exceptions import TulonaInvalidProfileConfigError, TulonaProfileException
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
    def profile_conf_path(self) -> str:
        return Path(self.get_profile_root, PROFILE_FILE_NAME)

    def validate_profile_config(self, profile_dict_raw: dict) -> bool:
        # TODO: implement validations for profile config
        valid = True

        if not valid:
            raise TulonaInvalidProfileConfigError(
                "Invalid profile config. Please check 'profiles.yml'"
            )

    def load_profile_config(self) -> None:
        profile_file_uri = self.profile_conf_path
        log.debug(f"Attempting to load profile config from {profile_file_uri}")

        if not path_exists(profile_file_uri):
            raise TulonaProfileException(
                f"Profile file {profile_file_uri} does not exist."
            )

        profile_dict_raw = read_yaml(profile_file_uri)

        if not isinstance(profile_dict_raw, dict):
            raise TulonaProfileException(
                f"{profile_file_uri} could not be parsed to a python dictionary."
            )

        log.debug(f"Profile config is successfully loaded from {profile_file_uri}")

        self.validate_profile_config(profile_dict_raw)

        return profile_dict_raw
