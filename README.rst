Tulona
======

Features
--------
* Compare databases


Development Environment Setup
-----------------------------
* For live installation execute `pip install --editable core`.


Sample profiles.yml
----------------------
.. code-block:: yaml
  :linenos:

  integration_project: # project_name
    profiles:
      pgdb:
        type: postgres
        host: db_pg
        port: 5432
        dbname: postgres
        username: postgres
        password: postgres
      mydb:
        type: mysql
        host: db_mysql
        port: 3306
        dbname: db
        username: user
        password: password
      snowflake:
        account: snowflake_account
        warehouse: dev_x_small
        database: dev_stage
        schema: user_schema
        user: dev_user
        private_key: 'rsa_key.p8'
        private_key_passphrase: 444444
      mssql:
        connection_string: 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=dagger;DATABASE=test;UID=user;PWD=password'

Sample tulona-project.yml
-------------------------
.. code-block:: yaml
  :linenos:

  version: '2.0'
  name: integration_project
  config-version: 1

  engine: pandas # supported engines: pandas
  outdir: output # the folder comparison result is written into

  # This is just the list of data sources, doesn't mean tulona will run tasks for all of them.
  # Datasources need to be picked in the CLI command to run tasks against.
  # Datasources can be present at database, schema as well as table level granularity.
  # Mentioning only database means both databases has (or should have) same schemas and tables.
  # Sames goes for skipping table names.
  # IMPORTANT: Names must be unique
  datasources:
    - name: postgres_postgres
      connection_profile: pgdb
      database: postgres
    - name: mysql_db
      connection_profile: mydb
      database: db
    - name: postgres_postgres_public
      connection_profile: pgdb
      database: postgres
      schema: public
    - name: mysql_db_db
      connection_profile: mydb
      database: db
      schema: db
    - name: postgres_postgres_public_reports
      connection_profile: pgdb
      database: postgres
      schema: public
      table: reports
    - name: mysql_db_db_reports
      connection_profile: mydb
      database: db
      schema: db
      table: reports
