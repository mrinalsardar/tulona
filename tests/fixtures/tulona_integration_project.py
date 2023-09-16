import pytest


tulona_integration_project__tulona_project_yml = """
version: '2.0'
name: integration_project
config-version: 2

connection_profiles:
  postgres1:
    type: postgres
    host: db_pg
    port: 5432
    username: postgres
    password: postgres
  mysql1:
    type: mysql
    host: db_mysql
    port: 3306
    username: user
    password: password

databases: # list of combinations databases to compare
  - postgres1: postgres
    mysql1: db


engine: pandas # supported engines: pandas & dask
ignore_schema: true # Don't go schema by schma. Compare all available tables in the database
outdir: output # folder comparison result is written into
"""

from ruamel.yaml import YAML
y = YAML(typ='safe')
print(y.load(tulona_integration_project__tulona_project_yml))
