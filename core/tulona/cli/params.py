import click


exec_engine = click.option(
    "--engine",
    help="Execution engine. Can be one of Pandas & Dask",
    type=click.STRING
)

ignore_schema = click.option(
    "--ignore-schema",
    help="Don't go schema by schma. Compare all available tables in the database",
    type=click.BOOL
)

outdir = click.option(
    "--outdir",
    help="Where do you want me to write the result of the comparison and other related metadata?",
    type=click.STRING
)