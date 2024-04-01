import click

exec_engine = click.option(
    "--engine", help="Execution engine. Can be one of Pandas right now", type=click.STRING
)

outdir = click.option(
    "--outdir",
    help="Directory to write the result of the comparison and other related metadata",
    type=click.STRING,
)

verbose = click.option("--verbose", "-v", is_flag=True, help="Show debug level logs")

datasources = click.option(
    "--datasources",
    help="Comma separated list of one or more datasource names defined in tulona-conf.yml file",
)

sample_count = click.option(
    "--sample-count", help="Number of maximum records to be compared"
)

compare = click.option(
    "--compare",
    is_flag=True,
    help="Can be used with profile task to compare profiles of different data sources",
)
