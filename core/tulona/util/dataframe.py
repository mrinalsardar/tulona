from typing import Union

import pandas as pd

from tulona.exceptions import TulonaFundamentalError


def apply_column_exclusion(
    df: pd.DataFrame, primary_key: str, exclude_columns: list, table: str
) -> Union[pd.DataFrame, None]:
    if primary_key in exclude_columns:
        raise TulonaFundamentalError(
            "Cannot exclude primary key/join key from data comparison"
        )

    missing_cols = []
    for col in exclude_columns:
        if col not in df.columns.tolist():
            missing_cols.append(col)

    if len(missing_cols) > 0:
        raise ValueError(
            f"Columns [{missing_cols}] to be excluded are not present in {table}"
        )

    df = df.drop(columns=exclude_columns)
    return df
