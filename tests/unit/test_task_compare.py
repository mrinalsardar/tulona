import pytest
import pandas as pd
import dask.dataframe as dd
from tulona.task.compare import CompareTask


class TestCompareTask:
    @pytest.fixture(scope='class')
    def instance(self):
        return CompareTask(profile={}, project={}, runtime={})


    def test_prepare_rows_pandas(self, instance):
        df_input = pd.DataFrame(
            data={
                "A": ["str1", "str2", "str3",],
                "B": [11, 22, 33,],
                "C": [1., 2., 3.,],
                "D": [True, False, True,]
            }
        )
        df_expected = pd.DataFrame(
            {
                "concat_value": [
                    "str1|||11|||1.0|||True", "str2|||22|||2.0|||False", "str3|||33|||3.0|||True"
                ],
                "row_hash": [ "1584926524221919774", "8113593925272518082", "7232552123411490637"
                ],
            }
        )
        df_actual = instance.prepare_rows_pandas(df=df_input, row_hash_col="row_hash")

        assert sorted(df_actual) == sorted(df_expected)

    def test_compare_tables_pandas(self, instance):
        df_input = pd.DataFrame(
            data={
                "A": ["str1", "str2", "str3",],
                "B": [11, 22, 33,],
                "C": [1., 2., 3.,],
                "D": [True, False, True,]
            }
        )

        

    def test_prepare_rows_dask(self, instance):
        df = pd.DataFrame(
            data={
                "A": ["str1", "str2", "str3",],
                "B": [11, 22, 33,],
                "C": [1., 2., 3.,],
                "D": [True, False, True,]
            }
        )
        df_input = dd.from_pandas(df, npartitions=2)
        df_expected = pd.DataFrame(
            {
                "concat_value": [
                    "str1|||11|||1.0|||True", "str2|||22|||2.0|||False", "str3|||33|||3.0|||True"
                ],
                "row_hash": [
                    "1584926524221919774", "8113593925272518082", "7232552123411490637"
                ],
            }
        )
        df_actual = instance.prepare_rows_dask(df=df_input, row_hash_col="row_hash")

        assert sorted(df_actual) == sorted(df_expected)
