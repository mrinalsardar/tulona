import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from tulona.exceptions import TulonaFundamentalError
from tulona.util.dataframe import apply_column_exclusion, get_sample_rows_for_each_value


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
    ],
)
def test_apply_column_exclusion(df, primary_key, exclude_columns, table, expected):
    actual = apply_column_exclusion(df, primary_key, exclude_columns, table)
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    "df,n_per_value,column_name,expected",
    [
        (
            pd.DataFrame(
                {
                    "A": [1, 2, 3, 4, 11, 12, 13, 21],
                    "B": ["a", "a", "a", "a", "b", "b", "b", "c"],
                }
            ),
            2,
            "B",
            [["a", 2], ["b", 2], ["c", 1]],
        ),
    ],
)
def test_get_sample_rows_for_each_value(df, n_per_value, column_name, expected):
    df = get_sample_rows_for_each_value(df, n_per_value, column_name)
    grouped = df.groupby(column_name).size().reset_index(name="row_count")
    actual = grouped.to_dict("split")["data"]
    assert actual == expected
