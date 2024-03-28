import time
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from tulona.task.base import BaseTask
from tulona.config.runtime import RunConfig
from typing import Dict, List
from tulona.util.profiles import get_connection_profile, extract_profile_name
from tulona.util.sql import (
    get_sample_row_query,
    get_query_output_as_df,
    build_filter_query_expression
)
from tulona.util.filesystem import create_dir_if_not_exist

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
                dbtype=self.profile['profiles'][extract_profile_name(self.project, datasource)]['type'],
                table_name=table_name
            )

        df = get_query_output_as_df(connection_manager=conman, query_text=query)
        return df


    def write_output(self, df: pd.DataFrame, datasource1, datasource2):
        df = df[sorted(df.columns.tolist())]
        ds1 = datasource1.replace('_', '')
        ds2 = datasource2.replace('_', '')
        outdir = create_dir_if_not_exist(self.project['outdir'])
        out_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        outfile = f"{ds1}_{ds2}_data_comparison_{out_timestamp}.xlsx"
        outfile_fqn = Path(outdir, outfile)
        log.debug(f"Writing output into: {outfile_fqn}")
        df.to_excel(outfile_fqn, sheet_name='Data Comparison', index=False)


    def execute(self):

        log.info("Starting task: Compare")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison works between two entities, not more, not less.")

        datasource1, datasource2 = self.datasources
        ds_dict1 = self.project['datasources'][datasource1]
        table_name1 = f"{ds_dict1['database']}.{ds_dict1['schema']}.{ds_dict1['table']}"
        ds_dict2 = self.project['datasources'][datasource2]
        table_name2 = f"{ds_dict2['database']}.{ds_dict2['schema']}.{ds_dict2['table']}"

        # Extract rows from both data sources
        log.debug("Extracting common data from both tables")
        if 'unique_key' in ds_dict1 and 'unique_key' in ds_dict2:
            i = 0
            while i < 10:
                log.debug(f"Extraction iteration: {i+1}")

                df1 = self.get_table_data(datasource=datasource1)
                if df1.shape[0] == 0:
                    raise ValueError(f"Table {table_name1} doesn't have any data")

                df1 = df1.rename(columns={c:c.lower() for c in df1.columns})

                if ds_dict1['unique_key'] not in df1.columns.tolist():
                    raise ValueError(
                        f"Unique key {ds_dict1['unique_key']} not present in {table_name1}"
                    )

                df2 = self.get_table_data(
                    datasource=datasource2,
                    query_expr=build_filter_query_expression(df1, ds_dict1['unique_key'])
                )

                df2 = df2.rename(columns={c:c.lower() for c in df2.columns})

                if ds_dict2['unique_key'] not in df2.columns.tolist():
                    raise ValueError(
                        f"Unique key {ds_dict2['unique_key']} not present in {table_name2}"
                    )

                if df2.shape[0] > 0:
                    df1 = df1[df1[ds_dict1['unique_key']].isin(df2[ds_dict1['unique_key']].tolist())]
                    break

                else:
                    datasource2, datasource1 = self.datasources
                    ds_dict1 = self.project['datasources'][datasource1]
                    table_name1 = f"{ds_dict1['database']}.{ds_dict1['schema']}.{ds_dict1['table']}"
                    ds_dict2 = self.project['datasources'][datasource2]
                    table_name2 = f"{ds_dict2['database']}.{ds_dict2['schema']}.{ds_dict2['table']}"

                i += 1

            if df1.shape[0] == 0:
                raise ValueError(
                    f"Could not find common data between {table_name1} and {table_name2}"
                )
        else:
            raise NotImplementedError("Table comparison without unique keys yet to be implementd")

        # Compare
        df1 = df1.rename(columns={c:c+'_'+datasource1.replace('_','') for c in df1.columns})
        df2 = df2.rename(columns={c:c+'_'+datasource2.replace('_','') for c in df2.columns})
        df_merge = pd.merge(
            left=df1 if i%2 == 0 else df2,
            right=df2 if i%2 == 0 else df1,
            left_on=ds_dict1['unique_key']+'_'+datasource1.replace('_',''),
            right_on=ds_dict2['unique_key']+'_'+datasource2.replace('_',''),
            validate='one_to_one'
        )

        log.debug("Writing comparison result")
        self.write_output(df_merge, datasource1, datasource2)

        end_time = time.time()
        log.info("Finished task: Compare")
        log.info(f"Total time taken: {end_time - start_time} seconds")
