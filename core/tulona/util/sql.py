from typing import Dict, List, Tuple, Union

import pandas as pd

from tulona.exceptions import TulonaNotImplementedError


def get_table_fqn(database: Union[str, None], schema: str, table: str) -> str:
    table_fqn = f"{database + '.' if database else ''}{schema}.{table}"
    return table_fqn


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
            f"Extracting sample rows from adapter type {dbtype} is not implemented."
        )

    return query


def get_column_query(table_fqn: str, columns: List[str], quoted=False):
    column_expr = ", ".join([f'"{c}"' if quoted else c for c in columns])
    query = f"""select {column_expr} from {table_fqn}"""

    return query


def get_query_output_as_df(connection_manager, query_text: str):  # pragma: no cover
    with connection_manager.engine.connect() as conn:
        df = pd.read_sql_query(query_text, conn)
    return df


def build_filter_query_expression(
    df: pd.DataFrame, primary_key: Union[List, Tuple, str], positive: bool = True
):
    expr_list = []
    primary_key = [primary_key] if isinstance(primary_key, str) else primary_key
    for k in primary_key:
        primary_key_values = df[k].tolist()

        if pd.api.types.is_numeric_dtype(df[k]):
            primary_key_values = [str(k) for k in primary_key_values]
            query_expr = f"""{k}{'' if positive else ' not'} in ({", ".join(primary_key_values)})"""
        else:
            query_expr = f"""{k}{'' if positive else ' not'} in ('{"', '".join(primary_key_values)}')"""

        expr_list.append(query_expr)

    final_expr = f" {'and' if positive else 'or'} ".join(expr_list)

    return final_expr


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


def get_metric_query(table_fqn, columns_dtype: Dict, metrics: list, quoted=False):
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

    query = f"""
    select
        {", ".join(call_funcs)}
    from {table_fqn}
    """

    return query


def get_table_data_query(dbtype, table_fqn, sample_count, query_expr: str = None):
    if query_expr:
        query = f"select * from {table_fqn} where {query_expr}"
    else:
        query = get_sample_row_query(
            dbtype=dbtype, table_name=table_fqn, sample_count=sample_count
        )
    return query
