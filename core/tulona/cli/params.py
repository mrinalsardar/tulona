import click


level = click.option(
    "--level",
    help="Which level of entity is to be compared? One of database, schema & table",
    type=click.STRING
)

exec_engine = click.option(
    "--engine",
    help="Execution engine. Can be one of Pandas & Dask",
    type=click.STRING
)

# ignore_schema = click.option(
#     "--ignore-schema",
#     help="Compare all available tables in the database. Works only with '--level' database",
#     type=click.BOOL
# )

outdir = click.option(
    "--outdir",
    help="Where do you want me to write the result of the comparison and other related metadata?",
    type=click.STRING
)