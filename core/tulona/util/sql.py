import pandas as pd

from tulona.exceptions import TulonaNotImplementedError


def get_sample_row_query(dbtype: str, table_name: str, sample_count: int):
    dbtype = dbtype.lower()

    # TODO: validate sampling mechanism for maximum possible randomness
    if dbtype in ["snowflake", "mssql"]:
        query = f"select * from {table_name} tablesample ({sample_count} rows)"
    elif dbtype == "postgres":
        # TODO: system_rows method not implemented, tablesample works for percentage selection
        # query = f"select * from {table_name} tablesample system_rows({sample_count})"
        query = f"select * from {table_name} limit {sample_count}"
    elif dbtype == "mysql":
        query = f"select * from {table_name} limit {sample_count}"
    else:
        raise TulonaNotImplementedError(
            f"Extracting sample rows from source type {dbtype} is not implemented."
        )

    return query


def get_column_query(table_name: str, column: str, quoted=False):
    if quoted:
        query = f"""select "{column}" from {table_name}"""
    else:
        query = f"""select {column} from {table_name}"""

    return query


def get_query_output_as_df(connection_manager, query_text: str):
    with connection_manager.engine.connect() as conn:
        df = pd.read_sql_query(query_text, conn)
    return df


def build_filter_query_expression(df: pd.DataFrame, primary_key: str):
    primary_keys = df[primary_key].tolist()

    if "int" in str(df[primary_key].dtype):
        primary_keys = [str(k) for k in primary_keys]
        query_expr = f"""{primary_key} in ({", ".join(primary_keys)})"""
    else:
        query_expr = f"""{primary_key} in ('{"', '".join(primary_keys)}')"""

    return query_expr
