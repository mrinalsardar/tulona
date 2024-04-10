import logging
from typing import List, Optional, Tuple, Union

import pandas as pd

from tulona.adapter.connection import ConnectionManager
from tulona.util.sql import (
    get_metadata_query,
    get_metric_query,
    get_query_output_as_df,
    get_table_fqn,
)

log = logging.getLogger(__name__)


def create_profile(
    database: str, schema: str, table: str, metrics: List[str], conman: ConnectionManager
) -> pd.DataFrame:
    # Extract metadata
    log.debug("Extracting metadata")
    meta_query = get_metadata_query(database, schema, table)
    log.debug(f"Executing query: {meta_query}")
    df_meta = get_query_output_as_df(connection_manager=conman, query_text=meta_query)
    df_meta = df_meta.rename(columns={c: c.lower() for c in df_meta.columns})

    # Extract metrics like min, max, avg, count, distinct count etc.
    log.debug("Extracting metrics")
    table_fqn = get_table_fqn(database, schema, table)
    metrics = list(map(lambda s: s.lower(), metrics))
    type_dict = df_meta[["column_name", "data_type"]].to_dict(orient="list")
    columns_dtype = {
        k: v for k, v in zip(type_dict["column_name"], type_dict["data_type"])
    }

    # TODO: quote for columns should be a config option, not an arbitrary thing
    try:
        log.debug("Trying query with unquoted column names")
        metric_query = get_metric_query(table_fqn, columns_dtype, metrics)
        log.debug(f"Executing query: {metric_query}")
        df_metric = get_query_output_as_df(
            connection_manager=conman, query_text=metric_query
        )
    except Exception as exp:
        log.warning(f"Previous query failed with error: {exp}")
        log.debug("Trying query with quoted column names")
        metric_query = get_metric_query(
            table_fqn,
            columns_dtype,
            metrics,
            quoted=True,
        )
        log.debug(f"Executing query: {metric_query}")
        df_metric = get_query_output_as_df(
            connection_manager=conman, query_text=metric_query
        )

    log.debug("Converting metric data into presentable format")
    metric_dict = {m: [] for m in ["column_name"] + metrics}
    for col in df_meta["column_name"]:
        metric_dict["column_name"].append(col)
        for m in metrics:
            try:
                metric_value = df_metric.iloc[0][f"{col}_{m}"]
            except Exception:
                metric_value = df_metric.iloc[0][f"{col.lower()}_{m}"]
            metric_dict[m].append(metric_value)
    df_metric = pd.DataFrame(metric_dict)

    # Combine meta and metric data
    df = pd.merge(left=df_meta, right=df_metric, how="inner", on="column_name")

    return df


# TODO: common param to toggle comparison result for common vs all columns
def perform_comparison(
    ds_compressed_names: List[str],
    dataframes: List[pd.DataFrame],
    on: str,
    how: str = "inner",
    suffixes: Tuple[str] = ("_x", "_y"),
    indicator: Union[bool, str] = False,
    validate: Optional[str] = None,
) -> pd.DataFrame:
    primary_key = on.lower()
    common_columns = {c.lower() for c in dataframes[0].columns.tolist()}

    dataframes_final = []
    for df in dataframes[1:]:
        colset = {c.lower() for c in df.columns.tolist()}
        common_columns = common_columns.intersection(colset)

    for ds_name, df in zip(ds_compressed_names, dataframes):
        df = df[list(common_columns)]
        df = df.rename(
            columns={
                c: f"{c}_{ds_name}" if c.lower() != primary_key else c.lower()
                for c in df.columns
            }
        )
        if pd.api.types.is_string_dtype(df[primary_key]):
            df[primary_key] = df[primary_key].str.lower()
        dataframes_final.append(df)

    df_merge = dataframes_final.pop()
    for df in dataframes_final:
        df_merge = pd.merge(
            left=df_merge,
            right=df,
            on=primary_key,
            how=how,
            suffixes=suffixes,
            indicator=indicator,
            validate=validate,
        )
    df_merge = df_merge[sorted(df_merge.columns.tolist())]

    return df_merge
