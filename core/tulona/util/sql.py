from typing import Dict

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


def get_metadata_query(database, schema, table):
    if database:
        query = f"""
        select * from information_schema.columns
        where upper(table_catalog) = '{database.upper()}'
        and upper(table_schema) = '{schema.upper()}'
        and upper(table_name) = '{table.upper()}'
        """
    else:
        query = f"""
        select * from information_schema.columns
        where upper(table_schema) = '{schema.upper()}'
        and upper(table_name) = '{table.upper()}'
        """
    return query


def get_metric_query(
    database, schema, table, columns_dtype: Dict, metrics: list, quoted=False
):
    numeric_types = [
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "double precision",
        "smallserial",
        "serial",
        "bigserial",
        "tinyint",
        "mediumint",
        "int",
        "float",
        "float4",
        "float8",
        "double",
        "number",
        "byteint",
        "bit",
        "smallmoney",
        "money",
    ]
    timestamp_types = [
        "timestamp",
        "date",
        "time",
        "year",
        "datetime",
        "interval",
        "datetimeoffset",
        "smalldatetime",
        "datetime2",
        "timestamp_tz",
        "timestamp_ltz",
        "timestamp_ntz",
        "timestamp with time zone",  # TODO probably incorrect representation
        "timestamp without time zone",
    ]

    numeric_funcs = [
        "min",
        "max",
        "average",
        "avg",
    ]
    timestamp_funcs = [
        "min",
        "max",
    ]
    generic_funcs = [
        "count",
        "distinct_count",
    ]

    function_map = {
        "min": "min({}) as {}_min",
        "max": "max({}) as {}_max",
        "avg": "avg({}) as {}_avg",
        "average": "avg({}) as {}_average",
        "count": "count({}) as {}_count",
        "distinct_count": "count(distinct({})) as {}_distinct_count",
    }

    call_funcs = []
    for col, dtype in columns_dtype.items():
        dtype = dtype.lower()
        qp = []
        for m in metrics:
            if (
                (m in numeric_funcs and dtype in numeric_types)
                or (m in timestamp_funcs and dtype in timestamp_types)
                or (m in generic_funcs)
            ):
                qp.append(
                    function_map[m.lower()].format(f'"{col}"' if quoted else col, col)
                )
            else:
                qp.append(f"'NA' as {col}_{m.lower()}")
        call_funcs.extend(qp)

    table_fqn = f"{database if database else ''}{'.' if database else ''}{schema}.{table}"

    query = f"""
    select
        {", ".join(call_funcs)}
    from {table_fqn}
    """

    return query
