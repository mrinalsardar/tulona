import click


level = click.option(
    "--level",
    help="Which level of entity is to be compared? One of database, schema & table",
    type=click.STRING,
)

exec_engine = click.option(
    "--engine",
    help="Execution engine. Can be one of Pandas & Dask",
    type=click.STRING
)

outdir = click.option(
    "--outdir",
    help="Directory to write the result of the comparison and other related metadata?",
    type=click.STRING,
)

verbose = click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show debug level logs"
)
