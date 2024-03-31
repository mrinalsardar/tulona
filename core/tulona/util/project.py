from typing import Dict


def extract_table_name_from_config(config: Dict, dbtype: str):
    dbtype = dbtype.lower()
    if dbtype in ("snowflake", "mssql", "postgres"):
        table = f"{config['database']}.{config['schema']}.{config['table']}"
    elif dbtype == "mysql":
        table = f"{config['schema']}.{config['table']}"

    return table
