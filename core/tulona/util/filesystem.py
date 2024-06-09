from datetime import datetime
from pathlib import Path
from typing import Dict, Union


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


def get_runid():
    out_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
    runid = f"runid__{out_timestamp}"
    return runid


def get_task_outdir(base_dir: str, runid: str, ds_list: list) -> Path:
    final_outdir = Path(base_dir, "_".join(ds_list), runid)
    return final_outdir


def get_task_outfile(task_conf: Dict) -> str:
    task = task_conf["task"].replace("-", "_")
    extra_params = []
    for p in task_conf:
        if p not in ["task", "datasources"]:
            if isinstance(task_conf[p], int):
                extra_params.extend([p.replace("_", ""), str(task_conf[p])])
            else:
                extra_params.append(p.replace("_", ""))

    param_str = ("_".join(extra_params) if len(extra_params) > 0 else "default").lower()
    filename = f"{task}__{param_str}.xlsx"
    return filename
