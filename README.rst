Tulona
======
A utility to compare tables, espacially useful to perform validations for migration projects.

.. list-table::
   :widths: 50 200

   * - Testing
     - |CI Test| |Deployment| |Coverage|
   * - Package
     - |PyPI Latest Release| |PyPI Downloads|
   * - Meta
     - |License Apache-2.0| |Codestyle Black|


Connection Profiles
-------------------
Connection profiles is a `yaml` file that will store credentials and other details to connect to the databases/data sources.

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


Features
--------
Executing `tulona` or `tulona -h` or `tulona --help` returns available commands.
All commands take one mandatory parameter, `--datasources`, a comma separated list of names of datasources from project config file (`tulona-project.yml`).

Tulona has following commands available:

* **compare-column**: To compare columns from tables from two sources/tables. This is expecially useful when you want see if all the rows from one table/source is present in the other one by comparing the primary/unique key. The result will be an excel file with extra primary/unique keys from both sides. If both have the same set of primary/unique keys, essentially means they have the same rows, excel file will be empty. Command samples:

  * Column[s] to compare is[are] specified in at least one of the datasource config in `tulona-project.yml` file with `compare_column` property:

    ``tulona compare-column --datasources employee_postgres,employee_mysql``
  * Column[s] to compare may/may not be specified in the datasource config in `tulona-project.yml` file with `compare_column` property(command line takes preference). In the command, column name is specified with the first data source, separated by colon(:):

    ``tulona compare-column --datasources employee_postgres:Employee_ID,employee_mysql``
  * Column[s] to compare may/may not be specified in the datasource config in `tulona-project.yml` file with `compare_column` property(command line takes preference). In the command, column name is specified with the second data source, separated by colon(:):

    ``tulona compare-column --datasources employee_postgres,employee_mysql:Employee_ID``
  * Column[s] to compare may/may not be specified in the datasource config in `tulona-project.yml` file with `compare_column` property(command line takes preference). In the command, column name is specified with both data sources, separated by colon(:):

    ``tulona compare-column --datasources employee_postgres:Employee_ID,employee_mysql:Employee_ID``

* **compare-data**: To compare sample data from two sources/tables. It will create a comparative view of all common columns from both sources/tables side by side (like: id_ds1 <-> id_ds2) and highlight mismatched values in the output excel file. By default it compares 20 common rows from both tables (subject to availabillity) but the number can be overridden with the command line argument `--sample-count`. Command samples:

  * Command without `--sample-count` parameter:

    ``tulona compare-data --datasources employee_postgres,employee_mysql``
  * Command with `--sample-count` parameter:

    ``tulona compare-data --sample-count 50 --datasources employee_postgres,employee_mysql``

* **ping**: To test connectivity to the databases for the datasources. Sample command:

  * To ping one data source pass the name to the `--datasources` parameter:

    ``tulona ping --datasources employee_postgres``

  * More than one datasources can be passed to the `--datasources` parameter separated by commas:

    ``tulona ping --datasources employee_postgres,employee_mysql``

* **profile**: To extract and compare metadata of two sources/tables. It includes metadata from `information_schema` related to the tables and some column level metrics (min, max, average, count & distinct_count). Sample commands:

  * Profiling without `--compare` flag. It will write metadata and metrics about different sources/tables in different sheets/tabs in the excel file (not a comparison view):

    ``tulona profile --datasources employee_postgres,employee_mysql``
  * Profiling with `--compare` flag. It will produce a comparison view (side by side):

    ``tulona profile --compare --datasources employee_postgres,employee_mysql``

To know more about any specific command, execute `tulona <command> -h`.


Development Environment Setup
-----------------------------
* For live installation execute `pip install --editable core`.


Build wheel executable
----------------------
* Execute `python -m build`.

Install wheel executable file
-----------------------------
* Execute `pip install <wheel-file.whl>`


.. |CI Test| image:: https://github.com/mrinalsardar/tulona/actions/workflows/test.yaml/badge.svg
   :target: https://github.com/mrinalsardar/tulona/actions/workflows/test.yaml
.. |Deployment| image:: https://github.com/mrinalsardar/tulona/actions/workflows/publish.yaml/badge.svg
   :target: https://github.com/mrinalsardar/tulona/actions/workflows/publish.yaml
.. |Coverage| image:: https://codecov.io/gh/mrinalsardar/tulona/graph/badge.svg?token=UGNjjgRskE
   :target: https://codecov.io/gh/mrinalsardar/tulona
   :alt: Coverage status
.. |PyPI Latest Release| image:: https://img.shields.io/pypi/v/tulona.svg
   :target: https://pypi.python.org/pypi/tulona/
.. |PyPI Downloads| image:: https://img.shields.io/pypi/dm/tulona.svg?label=PyPI%20downloads
   :target: https://pypi.org/project/tulona/
.. |License Apache-2.0| image:: https://img.shields.io/:license-Apache%202-brightgreen.svg
   :target: http://www.apache.org/licenses/LICENSE-2.0.txt
.. |Codestyle Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black