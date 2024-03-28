import click
import logging
from tulona.task.scan import ScanTask
from tulona.task.profile import ProfileTask
from tulona.task.compare import CompareDataTask
from tulona.task.test_connection import TestConnectionTask
from tulona.cli import params as p
from tulona.config.profile import Profile
from tulona.config.project import Project
from tulona.config.runtime import RunConfig


log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,  # TODO: Set level to INFO once verbosity is fixed
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
    """Tulona compares databases to find out differences"""


# command: tulona test-connection
@cli.command("test-connection")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def test_connection(ctx, **kwargs):
    """Scans data sources"""

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj['project']['name']]

    datasource_list = kwargs['datasources'].split(',')

    task = TestConnectionTask(ctx.obj["profile"], ctx.obj["project"], datasource_list)
    task.execute()


# command: tulona scan
@cli.command("scan")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def scan(ctx, **kwargs):
    """Scans data sources"""

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj['project']['name']]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    datasource_list = kwargs['datasources'].split(',')

    task = ScanTask(ctx.obj["profile"], ctx.obj["project"], ctx.obj["runtime"], datasource_list)
    task.execute()


# command: tulona profile
@cli.command("profile")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
def profile(ctx, **kwargs):
    """Scans data sources"""

    if kwargs["verbose"]:
        # TODO: Fix me
        # This setting doesn't enable debug level logging
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)

    prof = Profile()
    proj = Project()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.load_project_config()
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj['project']['name']]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    datasource_list = kwargs['datasources'].split(',')

    task = ProfileTask(ctx.obj["profile"], ctx.obj["project"], ctx.obj["runtime"], datasource_list)
    task.execute()


# command: tulona compare
@cli.command("compare-data")
@click.pass_context
@p.exec_engine
@p.outdir
@p.verbose
@p.datasources
@p.sample_count
def compare_data(ctx, **kwargs):
    """Compares two data entities"""

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
    ctx.obj["profile"] = prof.load_profile_config()[ctx.obj['project']['name']]
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    datasource_list = kwargs['datasources'].split(',')

    task = CompareDataTask(
        profile=ctx.obj["profile"],
        project=ctx.obj["project"],
        runtime=ctx.obj["runtime"],
        datasources=datasource_list,
        sample_count=kwargs['sample_count']
    )
    task.execute()


if __name__ == "__main__":
    cli()
