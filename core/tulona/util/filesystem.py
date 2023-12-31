from typing import Union
from pathlib import Path


def path_exists(p: Union[str, Path]) -> bool:
    return Path(p).exists()


def recursive_rmdir(directory):
    directory = Path(directory)
    for item in directory.iterdir():
        if item.is_dir():
            recursive_rmdir(item)
        else:
            item.unlink()
    directory.rmdir()


def create_or_replace_dir(d: Union[str, Path]) -> Path:
    p = Path(d)
    if p.exists():
        recursive_rmdir(p)
    p.mkdir()
    return p

def get_output_base_dir(base: str) -> Path:
    return create_or_replace_dir(base)

def get_result_dir(dir_dict: dict, base: Union[str, Path], key: str) -> Path:
    p = Path(get_output_base_dir(base), dir_dict[key])
    return create_or_replace_dir(p)


