import pandas as pd
import pytest

from tulona.exceptions import TulonaNotImplementedError
from tulona.util.sql import (
    build_filter_query_expression,
    get_column_query,
    get_information_schema_query,
    get_metric_query,
    get_sample_row_query,
    get_table_data_query,
    get_table_fqn,
)


@pytest.mark.parametrize(
    "database,schema,table,expected",
    [
        ("database", "schema", "table", "database.schema.table"),
        (None, "schema", "table", "schema.table"),
    ],
)
def test_get_table_fqn(database, schema, table, expected):
    table_fqn = get_table_fqn(database, schema, table)
    assert table_fqn == expected


@pytest.mark.parametrize(
    "dbtype,table_name,sample_count,expected",
    [
        (
            "snowflake",
            "database.schema.table",
            20,
            "select * from database.schema.table tablesample (20 rows)",
        ),
        (
            "mssql",
            "database.schema.table",
            20,
            "select top 20 * from database.schema.table",
        ),
        (
            "postgres",
            "database.schema.table",
            20,
            "select * from database.schema.table limit 20",
        ),
        (
            "mysql",
            "database.schema.table",
            20,
            "select * from database.schema.table limit 20",
        ),
        pytest.param(
            "unknown",
            "database.schema.table",
            20,
            "",
            marks=pytest.mark.xfail(
                raises=TulonaNotImplementedError,
                match="Extracting sample rows from source type",
            ),
        ),
    ],
)
def test_get_sample_row_query(dbtype, table_name, sample_count, expected):
    query = get_sample_row_query(dbtype, table_name, sample_count)
    assert query == expected


@pytest.mark.parametrize(
    "table_name,columns,quoted,expected",
    [
        (
            "database.schema.table",
            ["id"],
            True,
            """select "id" from database.schema.table""",
        ),
        (
            "database.schema.table",
            ["id"],
            False,
            """select id from database.schema.table""",
        ),
    ],
)
def test_get_column_query(table_name, columns, quoted, expected):
    query = get_column_query(table_name, columns, quoted)
    assert query == expected


@pytest.mark.parametrize(
    "df,primary_key,expected",
    [
        (
            pd.DataFrame(
                data={
                    "id": [1, 3, 4, 5],
                    "val": ["A", "C", "D", "E"],
                }
            ),
            "id",
            "id in (1, 3, 4, 5)",
        ),
        (
            pd.DataFrame(
                data={
                    "id": ["A", "C", "D", "E"],
                    "val": [1, 3, 4, 5],
                }
            ),
            "id",
            "id in ('A', 'C', 'D', 'E')",
        ),
    ],
)
def test_build_filter_query_expression(df, primary_key, expected):
    query_expr = build_filter_query_expression(df, primary_key)
    assert query_expr == expected


@pytest.mark.parametrize(
    "database,schema,table,info_view,dbtype,expected",
    [
        (
            "database",
            "schema",
            "table",
            "columns",
            "postgres",
            """
            select
                *
            from database.information_schema.columns
            where
                upper(table_schema) = 'SCHEMA'
                and upper(table_name) = 'TABLE'
        """,
        ),
        (
            "project",
            "dataset",
            "table",
            "columns",
            "bigquery",
            """
            select
                *
            from dataset.INFORMATION_SCHEMA.COLUMNS
            where
                upper(table_schema) = 'DATASET'
                and upper(table_name) = 'TABLE'
        """,
        ),
        (
            None,
            "schema",
            "table",
            "columns",
            "mysql",
            """
            select
                *
            from information_schema.columns
            where
                upper(table_schema) = 'SCHEMA'
                and upper(table_name) = 'TABLE'
        """,
        ),
    ],
)
def test_get_metadata_query(database, schema, table, info_view, dbtype, expected):
    query = get_information_schema_query(database, schema, table, info_view, dbtype)
    assert query.replace("\n", "").replace(" ", "") == expected.replace("\n", "").replace(
        " ", ""
    )


@pytest.mark.parametrize(
    "table_fqn,columns_dtype,metrics,quoted,expected",
    [
        # numeric function - numeric column
        (
            "database.schema.table",
            {
                "Age": "bigint",
            },
            [
                "max",
            ],
            False,
            """
    select
        max(cast(Age as decimal)) as Age_max
    from database.schema.table
    """,
        ),
        # timestamp function - timestamp column
        (
            "database.schema.table",
            {
                "Date_of_Birth": "datetime",
            },
            [
                "max",
            ],
            False,
            """
    select
        max(Date_of_Birth) as Date_of_Birth_max
    from database.schema.table
    """,
        ),
        # strictly numeric function - numeric column
        (
            "database.schema.table",
            {
                "Age": "bigint",
            },
            [
                "average",
            ],
            False,
            """
    select
        avg(cast(Age as decimal)) as Age_avg
    from database.schema.table
    """,
        ),
        # unsupported function - timestamp column
        (
            "database.schema.table",
            {
                "Date_of_Birth": "datetime",
            },
            [
                "average",
            ],
            False,
            """
    select
        'NA' as Date_of_Birth_average
    from database.schema.table
    """,
        ),
        # unsupported function - non numeric/timestamp column
        (
            "database.schema.table",
            {
                "Address": "text",
            },
            [
                "max",
            ],
            False,
            """
    select
        'NA' as Address_max
    from database.schema.table
    """,
        ),
        # unsupported function - non numeric/timestamp column
        (
            "database.schema.table",
            {
                "Address": "text",
            },
            [
                "avg",
            ],
            False,
            """
    select
        'NA' as Address_avg
    from database.schema.table
    """,
        ),
        # generic function - numeric column
        (
            "database.schema.table",
            {
                "Age": "bigint",
            },
            [
                "count",
            ],
            False,
            """
    select
        count(Age) as Age_count
    from database.schema.table
    """,
        ),
        # generic function - timestamp column
        (
            "database.schema.table",
            {
                "Date_of_Birth": "datetime",
            },
            [
                "count",
            ],
            False,
            """
    select
        count(Date_of_Birth) as Date_of_Birth_count
    from database.schema.table
    """,
        ),
        # generic function - non mumeric/timestamp column
        (
            "database.schema.table",
            {
                "Address": "text",
            },
            [
                "count",
            ],
            False,
            """
    select
        count(Address) as Address_count
    from database.schema.table
    """,
        ),
        # generic function - non mumeric/timestamp column [quoted]
        (
            "database.schema.table",
            {
                "Address": "text",
            },
            [
                "count",
            ],
            True,
            """
    select
        count("Address") as Address_count
    from database.schema.table
    """,
        ),
        # no database (mysql)
        (
            "schema.table",
            {
                "Address": "text",
            },
            [
                "count",
            ],
            False,
            """
    select
        count(Address) as Address_count
    from schema.table
    """,
        ),
        # unsupported function-datatype, quoting doesn't matter
        (
            "database.schema.table",
            {
                "Address": "text",
            },
            [
                "max",
            ],
            True,
            """
    select
        'NA' as Address_max
    from database.schema.table
    """,
        ),
    ],
)
def test_get_metric_query(table_fqn, columns_dtype, metrics, quoted, expected):
    query = get_metric_query(table_fqn, columns_dtype, metrics, quoted)
    assert query == expected


@pytest.mark.parametrize(
    "dbtype,table_fqn,sample_count,query_expr,expected",
    [
        (
            "postgres",
            "database.schema.table",
            20,
            "id in (1, 2)",
            "select * from database.schema.table where id in (1, 2)",
        ),
        (
            "postgres",
            "database.schema.table",
            20,
            None,
            "select * from database.schema.table limit 20",
        ),
    ],
)
def test_get_table_data_query(dbtype, table_fqn, sample_count, query_expr, expected):
    query = get_table_data_query(dbtype, table_fqn, sample_count, query_expr)
    assert query == expected
