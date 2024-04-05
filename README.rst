Tulona
======
A utility to compare tables, espacially useful for migration projects.

|Build Status| |Coverage|

Features
--------
* Profile tables
* Compare data of tables
* Compare column column of tables


Connection Profiles
-------------------
Connection profiles is a `yaml` file that will store credentials and other details to connect to the databases/data soruces.

It must be setup in `profiles.yml` file and it must be placed under `$HOME/.tulona` dierctory.
Create a directory named `.tulona` under your home directory and place `profiles.yml` under it.

This is what a sample `profiles.yml` looks like:

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
Project config file stores the properties of the tables that need to be compared.
It must be created in `tulona-project.yml` file and this file can be placed anywhere and that directory will be considered project root directory.
Which means that the `output`` folder will be created under that directory where all results will be stored.
It's always a good idea to create an empty directory and store `tulona-project.yml` under it.

This is how a `tulona-project.yml` file looks like:

.. code-block:: yaml

  version: '2.0'
  name: integration_project
  config-version: 1

  outdir: output # the folder comparison result is written into

  datasources:
    employee_postgres:
      connection_profile: pgdb
      database: postgres
      schema: public
      table: employee
      primary_key: employee_id
      exclude_columns:  # optional
        - name
      compare_column: Employee_ID  # conditional optional
    employee_mysql:
      connection_profile: mydb
      database: db
      schema: db
      table: employee
      primary_key: employee_id
      exclude_columns:  # optional
        - phone_number
      compare_column: Employee_ID  # conditional optional


Sample Commands
---------------
- tulona test-connection --datasources employee_postgres,employee_mysql
- tulona profile --datasources employee_postgres,employee_mysql
- tulona profile --compare --datasources employee_postgres,employee_mysql
- tulona compare-data --datasources employee_postgres,employee_mysql
- tulona compare-data --sample-count 50 --datasources employee_postgres,employee_mysql
- tulona compare-column --datasources employee_postgres,employee_mysql
- tulona compare-column --datasources employee_postgres:Employee_ID,employee_mysql
- tulona compare-column --datasources employee_postgres,employee_mysql:Employee_ID
- tulona compare-column --datasources employee_postgres:Employee_ID,employee_mysql:Employee_ID


Development Environment Setup
-----------------------------
* For live installation execute `pip install --editable core`.


Build wheel executable
----------------------
* Execute `python -m build`.

Install wheel executable file
-----------------------------
* Execute `pip install <wheel-file.whl>`


.. |Build Status| image:: https://github.com/mrinalsardar/tulona/actions/workflows/publish.yaml/badge.svg
   :target: https://github.com/mrinalsardar/tulona/actions/workflows/publish.yaml
.. |Coverage| image:: https://codecov.io/gh/mrinalsardar/tulona/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/mrinalsardar/tulona/branch/main
   :alt: Coverage status