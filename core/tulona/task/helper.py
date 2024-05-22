import logging
from typing import List, Optional, Tuple, Union

import pandas as pd

log = logging.getLogger(__name__)


# TODO: common param to toggle comparison result for common vs all columns
def perform_comparison(
    ds_compressed_names: List[str],
    dataframes: List[pd.DataFrame],
    on: Union[str, List],
    how: str = "inner",
    suffixes: Tuple[str] = ("_x", "_y"),
    indicator: Union[bool, str] = False,
    validate: Optional[str] = None,
    case_insensitive: bool = False,
) -> pd.DataFrame:
    on = [on] if isinstance(on, str) else on
    primary_key = [k.lower() for k in on]
    common_columns = {c.lower() for c in dataframes[0].columns.tolist()}

    dataframes_final = []
    for df in dataframes[1:]:
        colset = {c.lower() for c in df.columns.tolist()}
        common_columns = common_columns.intersection(colset)
    log.debug(f"Common columns: {common_columns}")

    for ds_name, df in zip(ds_compressed_names, dataframes):
        df = df[list(common_columns)]
        df = df.rename(
            columns={
                c: f"{c}-{ds_name}" if c.lower() not in primary_key else c
                for c in df.columns
            }
        )
        if case_insensitive:
            for k in primary_key:
                if pd.api.types.is_string_dtype(df[k]):
                    df[k] = df[k].str.lower()
        dataframes_final.append(df)

    df_merge = dataframes_final[0]
    for df in dataframes_final[1:]:
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
    new_columns = primary_key + [col for col in df_merge if col not in primary_key]
    df_merge = df_merge[new_columns]

    return df_merge
