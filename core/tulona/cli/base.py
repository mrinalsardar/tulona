import click
import logging
from tulona.task.compare import CompareTask
from tulona.cli import params as p
from tulona.config.porject import Project
from tulona.config.runtime import RunConfig


log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)

# command: tulona
@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    no_args_is_help=True,
    epilog="Execute: tulona <command> -h/--help for more help with specific commands"
)
@click.pass_context
def cli(ctx):
    """Tulona compares databases to find out differences"""


# command: tulona connect
@cli.command("connect")
@click.pass_context
def connect(ctx):
    """
    WIP - Tests the connection to all the database tools using
    the connection profiles from 'tulona_project.yml'
    """
    click.echo("Testing connections...")


# command: tulona compare
@cli.command("compare")
@click.pass_context
@p.exec_engine
@p.ignore_schema
@p.outdir
def compare(ctx, **kwargs):
    """Compares two data entities"""

    proj = Project()
    proj.load_project_config()

    ctx.obj = ctx.obj or {}
    ctx.obj["project"] = proj.project_config_raw
    ctx.obj["eligible_conn_profiles"] = proj.get_eligible_connection_profiles()
    ctx.obj["runtime"] = RunConfig(options=kwargs, project=ctx.obj["project"])

    task = CompareTask(ctx.obj["eligible_conn_profiles"], ctx.obj["runtime"])
    task.execute()



if __name__ == '__main__':
    cli()