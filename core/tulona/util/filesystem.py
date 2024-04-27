from datetime import datetime
from pathlib import Path
from typing import Union


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
def get_final_outdir(basedir: str, task_conf: str):
    out_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    task = task_conf["task"].replace("-", "")
    ds_list = [ds.split(":")[0].replace("_", "") for ds in task_conf["datasources"]]
    extra_params = []
    for p in task_conf:
        if p not in ["task", "datasources"]:
            if isinstance(task_conf[p], int):
                extra_params.extend([p.replace("_", ""), str(task_conf[p])])
            else:
                extra_params.append(p.replace("_", ""))

    innerdir = (
        f"{'_'.join(ds_list)}_{task}_{'_'.join(extra_params)}_{out_timestamp}".lower()
    )
    final_outdir = Path(basedir, innerdir)
    return final_outdir
