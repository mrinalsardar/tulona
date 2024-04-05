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


Connection Profiles
-------------------
Connection profiles must be setup in `profiles.yml` file and it must be placed under `$HOME/.tulona` dierctory.
This is how a sample `profiles.yml` looks like:

.. code-block:: yaml

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

Project Config File
-------------------
Project config must be created in `tulona-project.yml` file and this file can be placed anywhere.
The `output` folder will be create in the same dierctory where `tulona-project.yml` file is present.
This is how a `tulona-project.yml` file looks like:

.. code-block:: yaml

  version: '2.0'
  name: integration_project
  config-version: 1

  outdir: output # the folder comparison result is written into

  # This is just the list of data sources, doesn't mean tulona will run tasks for all of them.
  # Datasources need to be picked in the CLI command to run tasks against.
  datasources:
    employee_postgres:
      connection_profile: pgdb
      database: postgres
      schema: public
      table: employee
      primary_key: employee_id
      exclude_columns:
        - name
      compare_column: Employee_ID
    employee_mysql:
      connection_profile: mydb
      database: db
      schema: db
      table: employee
      primary_key: employee_id
      exclude_columns:
        - phone_number
      compare_column: Employee_ID

.. |Build Status| image:: https://github.com/mrinalsardar/tulona/actions/workflows/publish.yaml/badge.svg
   :target: https://github.com/mrinalsardar/tulona/actions/workflows/publish.yaml
.. |Coverage| image:: https://codecov.io/gh/mrinalsardar/tulona/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/mrinalsardar/tulona/branch/main
   :alt: Coverage status