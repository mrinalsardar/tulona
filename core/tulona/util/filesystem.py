from datetime import datetime
from pathlib import Path
from typing import List, Union


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


def create_dir_if_not_exist(d: Union[str, Path]) -> Path:
    p = Path(d)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_output_base_dir(base: str) -> Path:
    return create_or_replace_dir(base)


def get_result_dir(dir_dict: dict, base: Union[str, Path], key: str) -> Path:
    p = Path(get_output_base_dir(base), dir_dict[key])
    return create_or_replace_dir(p)


# TODO: Testable - pull current timestamp from caller
def get_final_outdir(basedir: str, ds_list: List[str]):
    out_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    final_outdir = Path(basedir, f"{'_'.join(ds_list)}_{out_timestamp}")
    return final_outdir
