import time
import logging
import pandas as pd
from dataclasses import dataclass
from tulona.task.base import BaseTask
from tulona.config.runtime import RunConfig
from typing import Dict, List, Union
from tulona.util.profiles import get_connection_profile, extract_profile_name
from tulona.util.sql import (
    get_sample_row_query,
    get_query_output_as_df,
    build_filter_query_expression
)

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    'sample_count': 20,
}

@dataclass
class CompareDataTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    sample_count: int=DEFAULT_VALUES['sample_count']
    unique_key: Union[str, List[str]]=None

    def get_table_data(self, datasource, query_expr: str=None):
        connection_profile = get_connection_profile(
            self.profile, self.project, datasource
        )
        conman = self.get_connection_manager(conn_profile=connection_profile)

        ds_dict = self.project['datasources'][datasource]
        table_name = f"{ds_dict['database']}.{ds_dict['schema']}.{ds_dict['table']}"
        if query_expr:
            query = f"select * from {table_name} where {query_expr}"
        else:
            query = get_sample_row_query(
                dbtype=self.profile['profiles'][extract_profile_name(self.project, datasource)],
                table_name=table_name
            )

        df = get_query_output_as_df(connection_manager=conman, query_text=query)
        return df


    def execute(self):

        log.info("Starting task: Compare")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison works between two entities, not more, not less.")

        datasource1, datasource2 = self.datasources
        if self.unique_key:
            for _ in range(10):
                df1 = self.get_table_data(datasource=datasource1)

                if df1.shape[0] == 0:
                    raise ValueError(f"Datasource {datasource1} doesn't have any data")

                df2 = self.get_table_data(
                    datasource=datasource2,
                    query_expr=build_filter_query_expression(df1, self.unique_key)
                )

                if df2.shape[0] > 0:
                    df1 = df1[df1[self.unique_key].isin(df2[self.unique_key].tolist())]
                    break
                else:
                    datasource2, datasource1 = self.datasources

            if df1.shape[0] == 0:
                raise ValueError(
                    f"Could not find common data between {datasource1} and {datasource2}"
                )
        else:
            pass

        end_time = time.time()
        log.info("Finished task: Compare")
        log.info(f"Total time taken: {end_time - start_time} seconds")
