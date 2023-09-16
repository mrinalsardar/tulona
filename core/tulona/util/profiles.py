from pathlib import Path


def profile_path() -> str:
    return Path(Path.home(), '.tulona', 'profiles.yml')


def profile_exists():
    return profile_path().exists()
