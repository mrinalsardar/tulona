import pandas as pd
import sqlalchemy as sa

def get_sample_row_query(dbtype: str, table_name: str):
    if dbtype.lower() == 'snowflake':
        query = f"SELECT * FROM {table_name} SAMPLE (10 ROWS)"

    return query

def get_query_output_as_df(connection_manager, query_text: str):
    with connection_manager.engine.begin() as conn:
        df = pd.read_sql_query(sa.text(query_text), conn)
    return df

def build_filter_query_expression(df: pd.DataFrame, unique_key: str):
    unique_keys = df[unique_key].tolist()
    query_expr = f"""{unique_key} in ('{"', '".join(unique_keys)}')"""
    return query_expr