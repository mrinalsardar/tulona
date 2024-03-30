import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingPrimaryKeyError
from tulona.task.base import BaseTask
from tulona.util.dataframe import apply_column_exclusion
from tulona.util.excel import highlight_mismatch_pair
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.project import extract_table_name_from_config
from tulona.util.sql import (build_filter_query_expression,
                             get_query_output_as_df, get_sample_row_query)

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    "sample_count": 20,
}


@dataclass
class CompareDataTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    sample_count: int

    # TODO: setting up default value for sample_count
    # Could find a good/simple way to do it with dataclass
    def get_sample_count(self):
        return self.sample_count if self.sample_count else DEFAULT_VALUES["sample_count"]

    def get_table_data(self, datasource, query_expr: str = None):
        connection_profile = get_connection_profile(self.profile, self.project, datasource)
        conman = self.get_connection_manager(conn_profile=connection_profile)

        ds_dict = self.project["datasources"][datasource]
        dbtype = self.profile["profiles"][extract_profile_name(self.project, datasource)]["type"]
        table_name = extract_table_name_from_config(config=ds_dict, dbtype=dbtype)

        if query_expr:
            query = f"select * from {table_name} where {query_expr}"
        else:
            query = get_sample_row_query(
                dbtype=dbtype, table_name=table_name, sample_count=self.get_sample_count()
            )

        df = get_query_output_as_df(connection_manager=conman, query_text=query)
        return df

    def write_output(self, df: pd.DataFrame, ds1, ds2):
        outdir = create_dir_if_not_exist(self.project["outdir"])
        out_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        outfile = f"{ds1}_{ds2}_data_comparison_{out_timestamp}.xlsx"
        outfile_fqn = Path(outdir, outfile)

        log.debug(f"Writing output into: {outfile_fqn}")
        df.to_excel(outfile_fqn, sheet_name="Data Comparison", index=False)

        log.debug("Highlighting mismtach pairs")
        highlight_mismatch_pair(excel_file=outfile_fqn, sheet="Data Comparison")

    def execute(self):
        log.info("Starting task: Compare")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison works between two entities, not more, not less.")

        datasource1, datasource2 = self.datasources
        ds_dict1 = self.project["datasources"][datasource1]
        ds_dict2 = self.project["datasources"][datasource2]

        table_name1 = f"{ds_dict1['database']}.{ds_dict1['schema']}.{ds_dict1['table']}"
        table_name2 = f"{ds_dict2['database']}.{ds_dict2['schema']}.{ds_dict2['table']}"

        # Extract rows from both data sources
        log.debug("Extracting common data from both tables")
        if "primary_key" in ds_dict1 and "primary_key" in ds_dict2:
            i = 0
            while i < 10:
                log.debug(f"Extraction iteration: {i+1}")

                df1 = self.get_table_data(datasource=datasource1)
                if df1.shape[0] == 0:
                    raise ValueError(f"Table {table_name1} doesn't have any data")

                df1 = df1.rename(columns={c: c.lower() for c in df1.columns})

                if ds_dict1["primary_key"] not in df1.columns.tolist():
                    raise ValueError(
                        f"Primary key {ds_dict1['primary_key']} not present in {table_name1}"
                    )

                df2 = self.get_table_data(
                    datasource=datasource2,
                    query_expr=build_filter_query_expression(df1, ds_dict1["primary_key"]),
                )

                df2 = df2.rename(columns={c: c.lower() for c in df2.columns})

                if ds_dict2["primary_key"] not in df2.columns.tolist():
                    raise ValueError(
                        f"Primary key {ds_dict2['primary_key']} not present in {table_name2}"
                    )

                if df2.shape[0] > 0:
                    df1 = df1[
                        df1[ds_dict1["primary_key"]].isin(df2[ds_dict1["primary_key"]].tolist())
                    ]
                    break

                else:
                    datasource2, datasource1 = self.datasources
                    ds_dict1 = self.project["datasources"][datasource1]
                    table_name1 = f"{ds_dict1['database']}.{ds_dict1['schema']}.{ds_dict1['table']}"
                    ds_dict2 = self.project["datasources"][datasource2]
                    table_name2 = f"{ds_dict2['database']}.{ds_dict2['schema']}.{ds_dict2['table']}"

                i += 1

            if df1.shape[0] == 0:
                raise ValueError(
                    f"Could not find common data between {table_name1} and {table_name2}"
                )
        else:
            raise TulonaMissingPrimaryKeyError("Primary key is required for data comparison")

        # Exclude columns
        log.debug("Excluding columns")
        df1 = apply_column_exclusion(df1, ds_dict1, table_name1)
        df2 = apply_column_exclusion(df2, ds_dict2, table_name2)

        # Compare
        common_columns = list(
            set(df1.columns)
            .intersection(set(df2.columns))
            .union({ds_dict1["primary_key"]})
            .union({ds_dict2["primary_key"]})
        )
        df1 = df1[common_columns].rename(
            columns={c: c + "_" + datasource1.replace("_", "") for c in df1.columns}
        )
        df2 = df2[common_columns].rename(
            columns={c: c + "_" + datasource2.replace("_", "") for c in df2.columns}
        )

        ds1_compressed = datasource1.replace("_", "")
        ds2_compressed = datasource2.replace("_", "")

        df_merge = pd.merge(
            left=df1 if i % 2 == 0 else df2,
            right=df2 if i % 2 == 0 else df1,
            left_on=ds_dict1["primary_key"] + "_" + ds1_compressed,
            right_on=ds_dict2["primary_key"] + "_" + ds2_compressed,
            validate="one_to_one",
        )

        df_merge = df_merge[sorted(df_merge.columns.tolist())]

        log.debug("Writing comparison result")
        self.write_output(df_merge, ds1_compressed, ds2_compressed)

        end_time = time.time()
        log.info("Finished task: Compare")
        log.info(f"Total time taken: {end_time - start_time} seconds")
