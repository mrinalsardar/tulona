import logging
from pathlib import Path

import click

from tulona.cli import params as p
from tulona.config.profile import Profile
from tulona.config.project import Project
from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingArgumentError
from tulona.task.compare import CompareColumnTask, CompareRowTask, CompareTask
from tulona.task.ping import PingTask
from tulona.task.profile import ProfileTask
from tulona.task.scan import ScanTask
from tulona.util.filesystem import get_final_outdir

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)


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


# command: tulona scan
@cli.command("scan")
@click.pass_context
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.compare
@p.sample_count
@p.composite
def scan(ctx, **kwargs):
    """Scan data sources to collect metadata"""

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
    if kwargs["compare"]:
        if kwargs["datasources"]:
            source_maps.append(kwargs["datasources"].split(","))
        elif "source_map" in ctx.obj["project"]:
            source_maps = ctx.obj["project"]["source_map"]
        else:
            raise TulonaMissingArgumentError(
                "Either --datasources command line argument or source_map (tulona-project.yml)"
                " must be provided for comparison with command: scan"
                " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
                " for more information on source_map"
            )
    else:
        if kwargs["datasources"]:
            source_maps.append(kwargs["datasources"].split(","))
        else:
            raise TulonaMissingArgumentError(
                "--datasources command line argument must be provided with command: scan"
                " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file"
                " for more information"
            )

    for datasource_list in source_maps:
        final_outdir = get_final_outdir(
            basedir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
        )

        task = ScanTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            final_outdir=final_outdir,
            compare=kwargs["compare"],
            sample_count=kwargs["sample_count"],
            composite=kwargs["composite"],
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
    if kwargs["compare"]:
        if kwargs["datasources"]:
            source_maps.append(kwargs["datasources"].split(","))
        elif "source_map" in ctx.obj["project"]:
            source_maps = ctx.obj["project"]["source_map"]
        else:
            raise TulonaMissingArgumentError(
                "Either --datasources command line argument or source_map (tulona-project.yml)"
                " must be provided for comparison with command: profile"
                " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
                " for more information on source_map"
            )
    else:
        if kwargs["datasources"]:
            source_maps.append(kwargs["datasources"].split(","))
        else:
            raise TulonaMissingArgumentError(
                "--datasources command line argument must be provided with command: profile"
                " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file"
                " for more information"
            )

    for datasource_list in source_maps:
        final_outdir = get_final_outdir(
            basedir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
        )
        outfile_fqn = Path(final_outdir, "profile_metadata.xlsx")

        task = ProfileTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
            compare=kwargs["compare"],
        )
        task.execute()


# command: tulona compare-row
@cli.command("compare-row")
@click.pass_context
# @p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.sample_count
def compare_row(ctx, **kwargs):
    """Compares rows from two data entities"""

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
            " must be provided with command: compare-row"
            " Check https://github.com/mrinalsardar/tulona/tree/main?tab=readme-ov-file#project-config-file"
            " for more information on source_map"
        )

    for datasource_list in source_maps:
        final_outdir = get_final_outdir(
            basedir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
        )
        outfile_fqn = Path(final_outdir, "row_comparison.xlsx")

        task = CompareRowTask(
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
@p.composite
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
        final_outdir = get_final_outdir(
            basedir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
        )
        outfile_fqn = Path(final_outdir, "column_comparison.xlsx")

        task = CompareColumnTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
            composite=kwargs["composite"],
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
@p.composite
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
        final_outdir = get_final_outdir(
            basedir=ctx.obj["project"]["outdir"],
            ds_list=[ds.split(":")[0].replace("_", "") for ds in datasource_list],
        )
        outfile_fqn = Path(final_outdir, "comparison.xlsx")

        task = CompareTask(
            profile=ctx.obj["profile"],
            project=ctx.obj["project"],
            runtime=ctx.obj["runtime"],
            datasources=datasource_list,
            outfile_fqn=outfile_fqn,
            sample_count=kwargs["sample_count"],
            composite=kwargs["composite"],
        )
        task.execute()


if __name__ == "__main__":
    cli()
