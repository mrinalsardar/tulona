import logging

import click

from tulona.cli import params as p
from tulona.config.profile import Profile
from tulona.config.project import Project
from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingArgumentError
from tulona.task.compare import CompareColumnTask, CompareDataTask

# from tulona.task.scan import ScanTask
from tulona.task.ping import PingTask
from tulona.task.profile import ProfileTask

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,  # TODO: Set level to INFO once verbosity is fixed
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)

# TODO: Make use of command line arguments like exec_engine, outdir etc.
# to override project config values.


# command: tulona
@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    no_args_is_help=True,
    epilog="Execute: tulona <command> -h/--help for more help with specific commands",
)
@click.pass_context
def cli(ctx):
    """Tulona compares data sources to find out differences"""


# command: tulona ping
@cli.command("ping")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def ping(ctx, **kwargs):
    """Test connectivity to datasources"""

    if "datasources" not in kwargs:
        raise TulonaMissingArgumentError(
            "--datasources argument must be provided with command: ping"
        )

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]

    datasource_list = kwargs["datasources"].split(",")

    task = PingTask(ctx.obj["profile"], ctx.obj["project"], datasource_list)
    task.execute()


# # command: tulona scan
# @cli.command("scan")
# @click.pass_context
# @p.exec_engine
# @p.outdir
# @p.verbose
# @p.datasources
# def scan(ctx, **kwargs):
#     """Scans data sources for schemas, tables, columns etc."""

#     if "datasources" not in kwargs:
#         raise TulonaMissingArgumentError(
#             "--datasources argument must be provided with command: scan"
#         )

#     if kwargs["verbose"]:
#         # TODO: Fix me
#         # This setting doesn't enable debug level logging
#         handler = logging.StreamHandler()
#         handler.setLevel(logging.DEBUG)

#     prof = Profile()
#     proj = Project()

#     ctx.obj = ctx.obj or {}
#     ctx.obj["project"] = proj.load_project_config()
#     ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
#     ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

#     datasource_list = kwargs["datasources"].split(",")

#     task = ScanTask(ctx.obj["profile"], ctx.obj["project"], ctx.obj["runtime"], datasource_list)
#     task.execute()


# command: tulona profile
@cli.command("profile")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.compare
def profile(ctx, **kwargs):
    """Profile data sources to collect metadata [row count, column min/max/mean etc.]"""

    if "datasources" not in kwargs:
        raise TulonaMissingArgumentError(
            "--datasources argument must be provided with command: profile"
        )

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    datasource_list = kwargs["datasources"].split(",")

    task = ProfileTask(
        ctx.obj["profile"],
        ctx.obj["project"],
        ctx.obj["runtime"],
        datasource_list,
        compare=kwargs["compare"],
    )
    task.execute()


# command: tulona compare-data
@cli.command("compare-data")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.sample_count
def compare_data(ctx, **kwargs):
    """Compares two data entities"""

    if "datasources" not in kwargs:
        raise TulonaMissingArgumentError(
            "--datasources argument must be provided with command: compare-data"
        )

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        )

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    datasource_list = kwargs["datasources"].split(",")

    task = CompareDataTask(
        profile=ctx.obj["profile"],
        project=ctx.obj["project"],
        runtime=ctx.obj["runtime"],
        datasources=datasource_list,
        sample_count=kwargs["sample_count"],
    )
    task.execute()


# command: tulona compare-column
@cli.command("compare-column")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def compare_column(ctx, **kwargs):
    """
    Column name must be specified for task: compare-column
    either by specifying 'compare_column' property in
    at least one of the datasource[project] configs
    (check sample tulona-project.yml file for example)
    or with '--datasources' command line argument
    using one of the following formats
    (column name is same for option 3 and 4):-
    1. <datasource1>:<col1>,<datasource2>:<col2>
    2. <datasource1>:<col>,<datasource2>:<col>
    3. <datasource1>:<col>,<datasource2>
    4. <datasource1>,<datasource2>:<col>
    """

    if "datasources" not in kwargs:
        raise TulonaMissingArgumentError(
            "--datasources argument must be provided with command: compare-column"
        )

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        )

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    datasource_list = kwargs["datasources"].split(",")

    task = CompareColumnTask(
        profile=ctx.obj["profile"],
        project=ctx.obj["project"],
        runtime=ctx.obj["runtime"],
        datasources=datasource_list,
    )
    task.execute()


if __name__ == "__main__":
    cli()
