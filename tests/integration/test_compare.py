from pathlib import Path
import pandas as pd
import pytest
from tulona.task.compare import CompareRowTask
from tulona.util.yaml_parser import read_yaml_string

profile_yml = """
integration_project: # project_name
  profiles:
    pgdb:
      type: postgres
      host: localhost
      port: 5432
      database: postgresdb
      username: tulona
      password: anolut
    mydb:
      type: mysql
      host: localhost
      port: 3306
      database: corporate
      username: tulona
      password: anolut
"""
full_profile = read_yaml_string(profile_yml)


@pytest.mark.parametrize(
    "profile,project,tconf,expected",
    [
        (
            full_profile["integration_project"],
            {
                "name": "integration_project",
                "datasources": {
                    "employee_postgres": {
                        "connection_profile": "pgdb",
                        "database": "postgres",
                        "schema": "corporate",
                        "table": "employee",
                        "primary_key": "Employee_ID",
                        "exclude_columns": ["Email", "Name"],
                        "compare_column": "Employee_ID",
                    },
                    "employee_mysql": {
                        "connection_profile": "mydb",
                        "schema": "corporate",
                        "table": "employee",
                        "primary_key": "Employee_ID",
                        "exclude_columns": ["Phone_Number"],
                        "compare_column": "Employee_ID",
                    },
                },
            },
            {
                "task": "compare-row",
                "datasources": ["employee_postgres", "employee_mysql"],
            },
            
        ),
    ],
)
def test_compare_row(profile, project, tconf, expected):

    task = CompareRowTask(
        profile=profile,
        project=project,
        datasources=tconf["datasources"],
        outfile_fqn=Path(""),
        sample_count=tconf["sample_count"] if "sample_count" in tconf else None,
        case_insensitive=(
            tconf["case_insensitive"] if "case_insensitive" in tconf else False
        ),
    )
