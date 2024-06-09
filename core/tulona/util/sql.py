from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from tulona.exceptions import TulonaNotImplementedError


def get_table_fqn(database: Optional[str], schema: str, table: str) -> str:
    table_fqn = f"{database + '.' if database else ''}{schema}.{table}"
    return table_fqn


def get_sample_row_query(dbtype: str, data_container: str, sample_count: int):
    dbtype = dbtype.lower()

    # TODO: validate sampling mechanism for maximum possible randomness
    if dbtype == "snowflake":
        query = f"select * from {data_container} tablesample ({sample_count} rows)"
    elif dbtype == "mssql":
        query = f"select top {sample_count} * from {data_container}"
    elif dbtype == "postgres":
        # TODO: system_rows method not implemented, tablesample works for percentage selection
        # query = f"select * from {table_name} tablesample system_rows({sample_count})"
        query = f"select * from {data_container} limit {sample_count}"
    elif dbtype == "mysql":
        query = f"select * from {data_container} limit {sample_count}"
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
    df: pd.DataFrame,
    primary_key: Union[List, Tuple, str],
    quoted: bool = False,
    positive: bool = True,
):
    expr_list = []
    primary_key = [primary_key] if isinstance(primary_key, str) else primary_key
    for k in primary_key:
        primary_key_values = df[k.lower()].tolist()

        if pd.api.types.is_numeric_dtype(df[k.lower()]):
            primary_key_values = [str(k) for k in primary_key_values]
            k = f'"{k}"' if quoted else k
            query_expr = f"""{k}{'' if positive else ' not'} in ({", ".join(primary_key_values)})"""
        else:
            k = f'"{k}"' if quoted else k
            query_expr = f"""{k}{'' if positive else ' not'} in ('{"', '".join(primary_key_values)}')"""

        expr_list.append(query_expr)

    final_expr = f" {'and' if positive else 'or'} ".join(expr_list)

    return final_expr


def get_information_schema_query(
    database: Union[str, None], schema: str, table: str, info_view: str
) -> str:
    query = f"""
        select
            *
        from {database + '.' if database else ''}information_schema.{info_view}
        where
            upper(table_schema) = '{schema.upper()}'
            and upper(table_name) = '{table.upper()}'
        """
    return query


def get_metric_query(data_container, columns_dtype: Dict, metrics: list, quoted=False):
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
        "timestamp with time zone",  # TODO: probably incorrect representation
        "timestamp without time zone",
    ]

    generic_function_map = {
        "count": "count({}) as {}_count",
        "distinct_count": "count(distinct({})) as {}_distinct_count",
    }
    numeric_function_map = {
        "min": "min(cast({} as decimal)) as {}_min",
        "max": "max(cast({} as decimal)) as {}_max",
        "avg": "avg(cast({} as decimal)) as {}_avg",
        "average": "avg(cast({} as decimal)) as {}_avg",
    }
    timestamp_function_map = {
        "min": "min({}) as {}_min",
        "max": "max({}) as {}_max",
    }

    call_funcs = []
    for col, dtype in columns_dtype.items():
        dtype = dtype.lower()
        qp = []
        for m in metrics:
            if m in generic_function_map:
                qp.append(
                    generic_function_map[m.lower()].format(
                        f'"{col}"' if quoted else col, col
                    )
                )
            elif m in numeric_function_map and dtype in numeric_types:
                qp.append(
                    numeric_function_map[m.lower()].format(
                        f'"{col}"' if quoted else col, col
                    )
                )
            elif m in timestamp_function_map and dtype in timestamp_types:
                qp.append(
                    timestamp_function_map[m.lower()].format(
                        f'"{col}"' if quoted else col, col
                    )
                )
            else:
                qp.append(f"'NA' as {col}_{m.lower()}")
        call_funcs.extend(qp)

    query = f"""
    select
        {", ".join(call_funcs)}
    from {data_container}
    """

    return query


def get_table_data_query(
    dbtype, data_container, sample_count, query_expr: Optional[str] = None
):
    if query_expr:
        query = f"select * from {data_container} where {query_expr}"
    else:
        query = get_sample_row_query(
            dbtype=dbtype, data_container=data_container, sample_count=sample_count
        )
    return query
