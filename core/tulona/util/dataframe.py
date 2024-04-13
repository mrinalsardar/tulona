from typing import List, Tuple, Union

import pandas as pd

from tulona.exceptions import TulonaFundamentalError


def apply_column_exclusion(
    df: pd.DataFrame,
    primary_key: Union[List, Tuple, str],
    exclude_columns: list,
    table: str,
) -> Union[pd.DataFrame, None]:
    for k in primary_key:
        if k in exclude_columns:
            raise TulonaFundamentalError(
                f"Cannot exclude primary key/join key {k} from comparison"
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
