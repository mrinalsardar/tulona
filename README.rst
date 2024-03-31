Tulona
======

|Build Status| |Coverage|

Features
--------
* Compare databases


Development Environment Setup
-----------------------------
* For live installation execute `pip install --editable core`.


Build wheel executable
----------------------
* Execute `python -m build` under root dierctory.

Install wheel executable file
-----------------------------
* Execute `pip install <wheel-file.whl>`


Sample profiles.yml
----------------------
.. code-block:: yaml
  :linenos:

  integration_project: # project_name
    profiles:
      pgdb:
        type: postgres
        host: localhost
        port: 5432
        database: postgres
        username: postgres
        password: postgres
      mydb:
        type: mysql
        host: localhost
        port: 3306
        database: db
        username: user
        password: password
      snowflake:
        type: snowflake
        account: snowflake_account
        warehouse: dev_x_small
        role: dev_role
        database: dev_stage
        schema: user_schema
        user: dev_user
        private_key: 'rsa_key.p8'
        private_key_passphrase: 444444
      mssql:
        type: mssql
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
  # Right now they are details about tables only.
  datasources:
    postgres_postgres_public_employee:
      connection_profile: pgdb
      database: postgres
      schema: public
      table: employee
      primary_key: employee_id
      exclude_columns:
        - name
      compare_column: Employee_ID
    mysql_db_db_employee:
      connection_profile: mydb
      database: db
      schema: db
      table: employee
      primary_key: employee_id
      exclude_columns:
        - phone_number
      compare_column: Employee_ID


.. |Build Status| image:: https://github.com/mrinalsardar/tulona/actions/workflows/tests.yaml/badge.svg
   :target: https://github.com/mrinalsardar/tulona/actions/workflows/tests.yaml
.. |Coverage| image:: https://codecov.io/gh/mrinalsardar/tulona/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/mrinalsardar/tulona/branch/main
   :alt: Coverage status