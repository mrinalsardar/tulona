import logging
import traceback
from datetime import datetime
from pathlib import Path

import click

from tulona.cli import params as p
from tulona.config.profile import Profile
from tulona.config.project import Project
from tulona.exceptions import TulonaMissingPropertyError
from tulona.task.compare import CompareColumnTask, CompareRowTask, CompareTask
from tulona.task.ping import PingTask
from tulona.task.profile import ProfileTask
from tulona.task.scan import ScanTask
from tulona.util.filesystem import get_runid, get_task_outdir, get_task_outfile

log = logging.getLogger()
log_formatter = logging.Formatter(
    "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)

consloe_handler = logging.StreamHandler()
consloe_handler.setFormatter(log_formatter)
consloe_handler.setLevel(logging.INFO)
log.addHandler(consloe_handler)

log_dir = Path(Path().absolute(), "log")
log_dir.mkdir(parents=True, exist_ok=True)
log_file_fqn = Path(log_dir, f"tulona_{datetime.now().strftime('%Y%m%d%H%M%S')}.log")
file_handler = logging.FileHandler(log_file_fqn)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)
log.addHandler(file_handler)

nlog = logging.getLogger(__name__)


# command: tulona
@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    no_args_is_help=True,
    epilog="Execute: tulona <command> -h/--help for more help with specific commands",
)
@click.pass_context
def cli(ctx):
    """Tulona compares data sources to find out differences"""
    logging.getLogger("tulona").setLevel(logging.DEBUG)
    nlog.info(f"Writing debug log into: {log_file_fqn}")

    prof = Profile()
    proj = Project()
    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["project"]["runid"] = get_runid()


# command: tulona ping
@cli.command("ping")
@click.pass_context
# @p.exec_engine
@p.datasources
def ping(ctx, **kwargs):
    """Test connectivity to datasources"""

    ping_tasks = []
    if kwargs["datasources"]:
        task_config = {
            "task": "ping",
            "datasources": kwargs["datasources"].split(","),
        }
        ping_tasks.append(task_config)
    else:
        ping_tasks = [t for t in ctx.obj["project"]["task_config"] if t["task"] == "ping"]

    if len(ping_tasks) == 0:
        raise RuntimeError(
            "Nothing to execute. Either define tasks in task_config section of the"
            " project config file or pass --datasources argument with values."
        )
    nlog.debug(f"Number of ping tasks to execute: {len(ping_tasks)}")

    for tconf in ping_tasks:
        nlog.info(f"Executing ping with task profile: {tconf}")
        PingTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
        ).execute()


# command: tulona scan
@cli.command("scan")
@click.pass_context
# @p.exec_engine
@p.datasources
@p.compare
@p.sample_count
@p.composite
@p.case_insensitive
def scan(ctx, **kwargs):
    """Scan data sources to collect metadata"""
    scan_tasks = []
    if kwargs["datasources"]:
        task_config = {
            "task": "scan",
            "datasources": kwargs["datasources"].split(","),
        }
        if kwargs["compare"]:
            task_config["compare"] = kwargs["compare"]
        if kwargs["sample_count"]:
            task_config["sample_count"] = kwargs["sample_count"]
        if kwargs["composite"]:
            task_config["composite"] = kwargs["composite"]
        if kwargs["case_insensitive"]:
            task_config["case_insensitive"] = kwargs["case_insensitive"]
        scan_tasks.append(task_config)
    else:
        scan_tasks = [t for t in ctx.obj["project"]["task_config"] if t["task"] == "scan"]

    if len(scan_tasks) == 0:
        raise RuntimeError(
            "Nothing to execute. Either define tasks in task_config section of the"
            " project config file or pass --datasources argument with values."
        )
    nlog.debug(f"Number of scan tasks to execute: {len(scan_tasks)}")

    for tconf in scan_tasks:
        nlog.info(f"Executing scan with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        nlog.debug(f"Output will be stored in: {final_outdir}")

        ScanTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            final_outdir=final_outdir,
            compare=tconf["compare"] if "compare" in tconf else None,
            sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
            composite=tconf["composite"] if "composite" in tconf else None,
            case_insensitive=(
                tconf["case_insensitive"] if "case_insensitive" in tconf else False
            ),
        ).execute()


# command: tulona profile
@cli.command("profile")
@click.pass_context
# @p.exec_engine
@p.datasources
@p.compare
def profile(ctx, **kwargs):
    """Profile data sources to collect metadata [row count, column min/max/mean etc.]"""
    profile_tasks = []
    if kwargs["datasources"]:
        task_config = {
            "task": "profile",
            "datasources": kwargs["datasources"].split(","),
        }
        if kwargs["compare"]:
            task_config["compare"] = kwargs["compare"]
        profile_tasks.append(task_config)
    else:
        profile_tasks = [
            t for t in ctx.obj["project"]["task_config"] if t["task"] == "profile"
        ]

    if len(profile_tasks) == 0:
        raise RuntimeError(
            "Nothing to execute. Either define tasks in task_config section of the"
            " project config file or pass --datasources argument with values."
        )
    nlog.debug(f"Number of profile tasks to execute: {len(profile_tasks)}")

    for tconf in profile_tasks:
        nlog.info(f"Executing profile with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        ProfileTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            outfile_fqn=outfile_fqn,
            compare=tconf["compare"] if "compare" in tconf else None,
        ).execute()


# command: tulona compare-row
@cli.command("compare-row")
@click.pass_context
# @p.exec_engine
@p.datasources
@p.sample_count
@p.case_insensitive
def compare_row(ctx, **kwargs):
    """Compares rows from two data entities"""
    compare_row_tasks = []
    if kwargs["datasources"]:
        task_config = {
            "task": "compare-row",
            "datasources": kwargs["datasources"].split(","),
        }
        if kwargs["sample_count"]:
            task_config["sample_count"] = kwargs["sample_count"]
        if kwargs["case_insensitive"]:
            task_config["case_insensitive"] = kwargs["case_insensitive"]
        compare_row_tasks.append(task_config)
    else:
        compare_row_tasks = [
            t for t in ctx.obj["project"]["task_config"] if t["task"] == "compare-row"
        ]

    if len(compare_row_tasks) == 0:
        raise RuntimeError(
            "Nothing to execute. Either define tasks in task_config section of the"
            " project config file or pass --datasources argument with values."
        )
    nlog.debug(f"Number compare-row tasks to execute: {len(compare_row_tasks)}")

    for tconf in compare_row_tasks:
        nlog.info(f"Executing compare-row with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        CompareRowTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            outfile_fqn=outfile_fqn,
            sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
            case_insensitive=(
                tconf["case_insensitive"] if "case_insensitive" in tconf else False
            ),
        ).execute()


# command: tulona compare-column
@cli.command("compare-column")
@click.pass_context
# @p.exec_engine
@p.datasources
@p.composite
@p.case_insensitive
def compare_column(ctx, **kwargs):
    """
    Column name must be specified for task: compare-column
    by specifying 'compare_column' property in
    all the datasource[project] configs
    (check sample tulona-project.yml file for example)
    """
    compare_column_tasks = []
    if kwargs["datasources"]:
        task_config = {
            "task": "compare-column",
            "datasources": kwargs["datasources"].split(","),
        }
        if kwargs["composite"]:
            task_config["composite"] = kwargs["composite"]
        if kwargs["case_insensitive"]:
            task_config["case_insensitive"] = kwargs["case_insensitive"]
        compare_column_tasks.append(task_config)
    else:
        compare_column_tasks = [
            t for t in ctx.obj["project"]["task_config"] if t["task"] == "compare-column"
        ]

    if len(compare_column_tasks) == 0:
        raise RuntimeError(
            "Nothing to execute. Either define tasks in task_config section of the"
            " project config file or pass --datasources argument with values."
        )
    nlog.debug(f"Number compare-column tasks to execute: {len(compare_column_tasks)}")

    for tconf in compare_column_tasks:
        nlog.info(f"Executing compare-column with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        CompareColumnTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            outfile_fqn=outfile_fqn,
            composite=tconf["composite"] if "composite" in tconf else None,
            case_insensitive=(
                tconf["case_insensitive"] if "case_insensitive" in tconf else False
            ),
        ).execute()


# command: tulona compare
@cli.command("compare")
@click.pass_context
# @p.exec_engine
@p.datasources
@p.sample_count
@p.composite
@p.case_insensitive
def compare(ctx, **kwargs):
    """
    Compare everything(profiles, rows and columns) for the given datasoures
    """
    compare_tasks = []
    if kwargs["datasources"]:
        task_config = {
            "task": "compare",
            "datasources": kwargs["datasources"].split(","),
        }
        if kwargs["sample_count"]:
            task_config["sample_count"] = kwargs["sample_count"]
        if kwargs["composite"]:
            task_config["composite"] = kwargs["composite"]
        if kwargs["case_insensitive"]:
            task_config["case_insensitive"] = kwargs["case_insensitive"]
        compare_tasks.append(task_config)
    else:
        compare_tasks = [
            t for t in ctx.obj["project"]["task_config"] if t["task"] == "compare"
        ]

    if len(compare_tasks) == 0:
        raise RuntimeError(
            "Nothing to execute. Either define tasks in task_config section of the"
            " project config file or pass --datasources argument with values."
        )
    nlog.debug(f"Number compare tasks to execute: {len(compare_tasks)}")

    for tconf in compare_tasks:
        nlog.info(f"Executing compare with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        CompareTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            outfile_fqn=outfile_fqn,
            sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
            composite=tconf["composite"] if "composite" in tconf else None,
            case_insensitive=(
                tconf["case_insensitive"] if "case_insensitive" in tconf else False
            ),
        ).execute()


# command: tulona run
@cli.command("run")
@click.pass_context
# @p.exec_engine
def run(ctx, **kwargs):
    """Run all tasks defined by `task_config` attribute in the project config file"""
    if "task_config" not in ctx.obj["project"]:
        raise TulonaMissingPropertyError(
            "Attribute `task_config` is not defined in project config"
        )

    # Extract tasks
    ping_tasks = [t for t in ctx.obj["project"]["task_config"] if t["task"] == "ping"]
    profile_tasks = [
        t for t in ctx.obj["project"]["task_config"] if t["task"] == "profile"
    ]
    compare_row_tasks = [
        t for t in ctx.obj["project"]["task_config"] if t["task"] == "compare-row"
    ]
    compare_column_tasks = [
        t for t in ctx.obj["project"]["task_config"] if t["task"] == "compare-column"
    ]
    compare_tasks = [
        t for t in ctx.obj["project"]["task_config"] if t["task"] == "compare"
    ]
    scan_tasks = [t for t in ctx.obj["project"]["task_config"] if t["task"] == "scan"]

    # PingTask
    for tconf in ping_tasks:
        nlog.info(f"Executing ping with task profile: {tconf}")
        PingTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
        ).execute()

    # ProfileTask
    for tconf in profile_tasks:
        nlog.info(f"Executing profile with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        try:
            ProfileTask(
                profile=ctx.obj["profile"],
                project=ctx.obj["project"],
                datasources=tconf["datasources"],
                outfile_fqn=outfile_fqn,
                compare=tconf["compare"] if "compare" in tconf else None,
            ).execute()
        except Exception:
            log.error(f"Profiling failed with error: {traceback.format_exc()}")

    # CompareRowTask
    for tconf in compare_row_tasks:
        nlog.info(f"Executing compare-row with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        try:
            CompareRowTask(
                profile=ctx.obj["profile"],
                project=ctx.obj["project"],
                datasources=tconf["datasources"],
                outfile_fqn=outfile_fqn,
                sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
                case_insensitive=(
                    tconf["case_insensitive"] if "case_insensitive" in tconf else False
                ),
            ).execute()
        except Exception:
            log.error(f"Row comparison failed with error: {traceback.format_exc()}")

    # CompareColumnTask
    for tconf in compare_column_tasks:
        nlog.info(f"Executing compare-column with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        try:
            CompareColumnTask(
                profile=ctx.obj["profile"],
                project=ctx.obj["project"],
                datasources=tconf["datasources"],
                outfile_fqn=outfile_fqn,
                composite=tconf["composite"] if "composite" in tconf else None,
                case_insensitive=(
                    tconf["case_insensitive"] if "case_insensitive" in tconf else False
                ),
            ).execute()
        except Exception:
            log.error(f"Column comparison failed with errorr: {traceback.format_exc()}")

    # CompareTask
    for tconf in compare_tasks:
        nlog.info(f"Executing compare with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        outfilename = get_task_outfile(task_conf=tconf)
        outfile_fqn = Path(final_outdir, outfilename)
        nlog.debug(f"Output will be stored in: {final_outdir}")

        CompareTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            outfile_fqn=outfile_fqn,
            sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
            composite=tconf["composite"] if "composite" in tconf else None,
            case_insensitive=(
                tconf["case_insensitive"] if "case_insensitive" in tconf else False
            ),
        ).execute()

    # ScanTask
    for tconf in scan_tasks:
        nlog.info(f"Executing scan with task profile: {tconf}")
        final_outdir = get_task_outdir(
            base_dir=ctx.obj["project"]["outdir"],
            runid=ctx.obj["project"]["runid"],
            ds_list=tconf["datasources"],
        )
        nlog.debug(f"Output will be stored in: {final_outdir}")

        ScanTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            datasources=tconf["datasources"],
            final_outdir=final_outdir,
            compare=tconf["compare"] if "compare" in tconf else None,
            sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
            composite=tconf["composite"] if "composite" in tconf else None,
            case_insensitive=(
                tconf["case_insensitive"] if "case_insensitive" in tconf else False
            ),
        ).execute()


if __name__ == "__main__":
    cli()
