import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from tulona.exceptions import TulonaFundamentalError
from tulona.util.dataframe import apply_column_exclusion


@pytest.mark.parametrize(
    "df,primary_key,exclude_columns,table,expected",
    [
        (
            pd.DataFrame(
                {
                    "A": [1, 2, 3, 4],
                    "B": ["a", "b", "c", "d"],
                    "C": [True, False, True, False],
                    "D": [0.1, 0.2, 0.3, 0.4],
                }
            ),
            "A",
            ["B", "C"],
            "table",
            pd.DataFrame(
                {
                    "A": [1, 2, 3, 4],
                    "D": [0.1, 0.2, 0.3, 0.4],
                }
            ),
        ),
        pytest.param(
            pd.DataFrame(
                {
                    "A": [1, 2, 3, 4],
                    "B": ["a", "b", "c", "d"],
                    "C": [True, False, True, False],
                }
            ),
            "A",
            ["A", "C"],
            "table",
            None,
            marks=pytest.mark.xfail(
                raises=TulonaFundamentalError,
                match="Cannot exclude primary key/join key",
            ),
        ),
        pytest.param(
            pd.DataFrame(
                {
                    "A": [1, 2, 3, 4],
                    "B": ["a", "b", "c", "d"],
                    "C": [True, False, True, False],
                }
            ),
            "A",
            ["D"],
            "table",
            None,
            marks=pytest.mark.xfail(
                raises=ValueError,
                match="to be excluded are not present in",
            ),
        ),
    ],
)
def test_apply_column_exclusion(df, primary_key, exclude_columns, table, expected):
    actual = apply_column_exclusion(df, primary_key, exclude_columns, table)
    assert_frame_equal(actual, expected)
