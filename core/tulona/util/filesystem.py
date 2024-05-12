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


def get_run_result_dir(basedir: str):
    out_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
    run_dir = Path(basedir, f"runid__{out_timestamp}")
    return run_dir


# TODO: Testable - pull current timestamp from caller
def get_task_outdir(run_dir: str, task_conf: str):
    task = task_conf["task"].replace("-", "")
    ds_list = [ds.split(":")[0].replace("_", "") for ds in task_conf["datasources"]]
    extra_params = []
    for p in task_conf:
        if p not in ["task", "datasources"]:
            if isinstance(task_conf[p], int):
                extra_params.extend([p.replace("_", ""), str(task_conf[p])])
            else:
                extra_params.append(p.replace("_", ""))

    innerdir = f"{'_'.join(ds_list)}_{task}{'_' + '_'.join(extra_params) if len(extra_params) > 0 else ''}".lower()
    final_outdir = Path(run_dir, innerdir)
    return final_outdir
