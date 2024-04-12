import logging

import click

from tulona.cli import params as p
from tulona.config.profile import Profile
from tulona.config.project import Project
from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingArgumentError
from tulona.task.compare import CompareColumnTask, CompareDataTask, CompareTask

# from tulona.task.scan import ScanTask
from tulona.task.ping import PingTask
from tulona.task.profile import ProfileTask
from tulona.util.filesystem import get_outfile_fqn

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
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
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def ping(ctx, **kwargs):
    """Test connectivity to datasources"""

    if kwargs["verbose"]:
        logging.getLogger("tulona").setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]

    if kwargs["datasources"]:
        datasource_list = kwargs["datasources"].split(",")
    else:
        datasource_list = list(ctx.obj["project"]["datasources"].keys())

    task = PingTask(
        profile=ctx.obj["profile"],
        project=ctx.obj["project"],
        datasources=datasource_list,
    )
    task.execute()


# command: tulona profile
@cli.command("profile")
@click.pass_context
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.compare
def profile(ctx, **kwargs):
    """Profile data sources to collect metadata [row count, column min/max/mean etc.]"""

    if kwargs["verbose"]:
        logging.getLogger("tulona").setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    # Override config outdir if provided in command line
    if kwargs["outdir"]:
        ctx.obj["project"]["outdir"] = kwargs["outdir"]

    source_maps = []
    if kwargs["datasources"]:
        source_maps.append(kwargs["datasources"].split(","))
    elif "source_map" in ctx.obj["project"]:
        source_maps = ctx.obj["project"]["source_map"]
    else:
        raise TulonaMissingArgumentError(
            "Either --datasources command line argument or source_map (tulona-project.yml)"
            " must be provided with command: profile"
            " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
            " for more information on source_map"
        )

    for datasource_list in source_maps:
        outfile_fqn = get_outfile_fqn(
            outdir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
            infix="profiling",
        )

        task = ProfileTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
            compare=kwargs["compare"],
        )
        task.execute()


# command: tulona compare-data
@cli.command("compare-data")
@click.pass_context
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.sample_count
def compare_data(ctx, **kwargs):
    """Compares two data entities"""

    if kwargs["verbose"]:
        logging.getLogger("tulona").setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    # Override config outdir if provided in command line
    if kwargs["outdir"]:
        ctx.obj["project"]["outdir"] = kwargs["outdir"]

    source_maps = []
    if kwargs["datasources"]:
        source_maps.append(kwargs["datasources"].split(","))
    elif "source_map" in ctx.obj["project"]:
        source_maps = ctx.obj["project"]["source_map"]
    else:
        raise TulonaMissingArgumentError(
            "Either --datasources command line argument or source_map (tulona-project.yml)"
            " must be provided with command: compare-data"
            " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
            " for more information on source_map"
        )

    for datasource_list in source_maps:
        outfile_fqn = get_outfile_fqn(
            outdir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
            infix="data_comparison",
        )

        task = CompareDataTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
            sample_count=kwargs["sample_count"],
        )
        task.execute()


# command: tulona compare-column
@cli.command("compare-column")
@click.pass_context
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def compare_column(ctx, **kwargs):
    """
    Column name must be specified for task: compare-column
    by specifying 'compare_column' property in
    all the datasource[project] configs
    (check sample tulona-project.yml file for example)
    """

    if kwargs["verbose"]:
        logging.getLogger("tulona").setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    # Override config outdir if provided in command line
    if kwargs["outdir"]:
        ctx.obj["project"]["outdir"] = kwargs["outdir"]

    source_maps = []
    if kwargs["datasources"]:
        source_maps.append(kwargs["datasources"].split(","))
    elif "source_map" in ctx.obj["project"]:
        source_maps = ctx.obj["project"]["source_map"]
    else:
        raise TulonaMissingArgumentError(
            "Either --datasources command line argument or source_map (tulona-project.yml)"
            " must be provided with command: compare-column"
            " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
            " for more information on source_map"
        )

    for datasource_list in source_maps:
        outfile_fqn = get_outfile_fqn(
            outdir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
            infix="column_comparison",
        )

        task = CompareColumnTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
        )
        task.execute()


# command: tulona compare
@cli.command("compare")
@click.pass_context
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.sample_count
def compare(ctx, **kwargs):
    """
    Compare everything(profiles, rows and columns) for the given datasoures
    """

    if kwargs["verbose"]:
        logging.getLogger("tulona").setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj["project"]["name"]]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    # Override config outdir if provided in command line
    if kwargs["outdir"]:
        ctx.obj["project"]["outdir"] = kwargs["outdir"]

    source_maps = []
    if kwargs["datasources"]:
        source_maps.append(kwargs["datasources"].split(","))
    elif "source_map" in ctx.obj["project"]:
        source_maps = ctx.obj["project"]["source_map"]
    else:
        raise TulonaMissingArgumentError(
            "Either --datasources command line argument or source_map (tulona-project.yml)"
            " must be provided with command: compare"
            " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
            " for more information on source_map"
        )

    for datasource_list in source_maps:
        outfile_fqn = get_outfile_fqn(
            outdir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
            infix="comparison",
        )

        task = CompareTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
            sample_count=kwargs["sample_count"],
        )
        task.execute()


if __name__ == "__main__":
    cli()
