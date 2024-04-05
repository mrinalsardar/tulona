import logging
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingPrimaryKeyError, TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.util.dataframe import apply_column_exclusion
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.project import extract_table_name_from_config
from tulona.util.sql import (
    build_filter_query_expression,
    get_column_query,
    get_query_output_as_df,
    get_sample_row_query,
)

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
    sample_count: int = DEFAULT_VALUES["sample_count"]

    # Support for default values
    def __post_init__(self):
        for field in fields(self):
            # If there is a default and the value of the field is none we can assign a value
            if (
                not isinstance(field.default, _MISSING_TYPE)
                and getattr(self, field.name) is None
            ):
                setattr(self, field.name, field.default)

    # TODO: needs refactoring to remove duplicate some code
    def get_table_data(self, datasource, query_expr: str = None):
        connection_profile = get_connection_profile(
            self.profile, self.project, datasource
        )
        conman = self.get_connection_manager(conn_profile=connection_profile)

        ds_dict = self.project["datasources"][datasource]
        dbtype = self.profile["profiles"][extract_profile_name(self.project, datasource)][
            "type"
        ]
        table_name = extract_table_name_from_config(config=ds_dict, dbtype=dbtype)

        if query_expr:
            query = f"select * from {table_name} where {query_expr}"
        else:
            query = get_sample_row_query(
                dbtype=dbtype, table_name=table_name, sample_count=self.sample_count
            )

        df = get_query_output_as_df(connection_manager=conman, query_text=query)
        return df

    def get_outfile_fqn(self, ds_list):
        outdir = create_dir_if_not_exist(self.project["outdir"])
        out_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        outfile = f"{'_'.join(ds_list)}_data_comparison_{out_timestamp}.xlsx"
        outfile_fqn = Path(outdir, outfile)
        return outfile_fqn

    def execute(self):
        log.info("Starting task: Compare")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison works between two entities, not more, not less.")

        datasource1, datasource2 = self.datasources
        ds_dict1 = self.project["datasources"][datasource1]
        ds_dict2 = self.project["datasources"][datasource2]

        dbtype1 = self.profile["profiles"][
            extract_profile_name(self.project, datasource1)
        ]["type"]
        dbtype2 = self.profile["profiles"][
            extract_profile_name(self.project, datasource2)
        ]["type"]
        table_name1 = extract_table_name_from_config(config=ds_dict1, dbtype=dbtype1)
        table_name2 = extract_table_name_from_config(config=ds_dict2, dbtype=dbtype2)

        # Extract rows from both data sources
        log.debug(
            f"Trying to extract {self.sample_count} common records from both data sources"
        )
        if "primary_key" in ds_dict1 and "primary_key" in ds_dict2:
            i = 0
            while i < 10:
                log.debug(f"Extraction iteration: {i + 1}")

                df1 = self.get_table_data(datasource=datasource1)
                if df1.shape[0] == 0:
                    raise ValueError(f"Table {table_name1} doesn't have any data")

                df1 = df1.rename(columns={c: c.lower() for c in df1.columns})

                if ds_dict1["primary_key"].lower() not in df1.columns.tolist():
                    raise ValueError(
                        f"Primary key {ds_dict1['primary_key'].lower()} not present in {table_name1}"
                    )

                df2 = self.get_table_data(
                    datasource=datasource2,
                    query_expr=build_filter_query_expression(
                        df1, ds_dict1["primary_key"].lower()
                    ),
                )

                df2 = df2.rename(columns={c: c.lower() for c in df2.columns})

                if ds_dict2["primary_key"].lower() not in df2.columns.tolist():
                    raise ValueError(
                        f"Primary key {ds_dict2['primary_key'].lower()} not present in {table_name2}"
                    )

                if df2.shape[0] > 0:
                    df1 = df1[
                        df1[ds_dict1["primary_key"].lower()].isin(
                            df2[ds_dict1["primary_key"].lower()].tolist()
                        )
                    ]
                    break

                else:
                    datasource2, datasource1 = self.datasources
                    dbtype1 = self.profile["profiles"][
                        extract_profile_name(self.project, datasource1)
                    ]["type"]
                    dbtype2 = self.profile["profiles"][
                        extract_profile_name(self.project, datasource2)
                    ]["type"]
                    table_name1 = extract_table_name_from_config(
                        config=ds_dict1, dbtype=dbtype1
                    )
                    table_name2 = extract_table_name_from_config(
                        config=ds_dict2, dbtype=dbtype2
                    )

                i += 1

            if df1.shape[0] == 0:
                raise ValueError(
                    f"Could not find common data between {table_name1} and {table_name2}"
                )
        else:
            raise TulonaMissingPrimaryKeyError(
                "Primary key is required for data comparison"
            )

        # Exclude columns
        log.debug("Excluding columns")
        if "exclude_columns" in ds_dict1:
            df1 = apply_column_exclusion(df1, ds_dict1, table_name1)
        if "exclude_columns" in ds_dict2:
            df2 = apply_column_exclusion(df2, ds_dict2, table_name2)

        # Compare
        common_columns = list(
            set(df1.columns)
            .intersection(set(df2.columns))
            .union({ds_dict1["primary_key"].lower()})
            .union({ds_dict2["primary_key"].lower()})
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
            left_on=ds_dict1["primary_key"].lower() + "_" + ds1_compressed,
            right_on=ds_dict2["primary_key"].lower() + "_" + ds2_compressed,
            validate="one_to_one",
        )

        df_merge = df_merge[sorted(df_merge.columns.tolist())]

        outfile_fqn = self.get_outfile_fqn([ds1_compressed, ds2_compressed])
        log.debug("Writing comparison result into: {outfile_fqn}")
        df_merge.to_excel(outfile_fqn, sheet_name="Data Comparison", index=False)

        log.debug("Highlighting mismtach cells")
        highlight_mismatch_cells(
            excel_file=outfile_fqn, sheet="Data Comparison", num_ds=len(self.datasources)
        )

        end_time = time.time()
        log.info("Finished task: Compare")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")


@dataclass
class CompareColumnTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]

    def get_column_data(self, datasource, table, column):
        connection_profile = get_connection_profile(
            self.profile, self.project, datasource
        )
        conman = self.get_connection_manager(conn_profile=connection_profile)

        query = get_column_query(table, column)
        try:
            log.debug(f"Trying unquoted column name: {column}")
            df = get_query_output_as_df(connection_manager=conman, query_text=query)
        except Exception as exp:
            log.debug(f"Failed with error: {exp}")
            log.debug(f'Trying quoted column name: "{column}"')
            query = get_column_query(table, column, quoted=True)
            df = get_query_output_as_df(connection_manager=conman, query_text=query)
        return df

    def write_result(self, df, ds1, ds2):
        outdir = create_dir_if_not_exist(self.project["outdir"])
        out_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        outfile = f"{ds1}_{ds2}_column_comparison_{out_timestamp}.xlsx"
        outfile_fqn = Path(outdir, outfile)

        log.debug(f"Writing output into: {outfile_fqn}")
        df.to_excel(outfile_fqn, sheet_name="Column Comparison", index=False)

    def execute(self):
        log.info("Starting task: compare-column")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison works between two entities, not more, not less.")

        datasource1, datasource2 = self.datasources
        if ":" in datasource1 and ":" in datasource2:
            datasource1, column1 = datasource1.split(":")
            datasource2, column2 = datasource2.split(":")
        elif ":" in datasource1:
            datasource1, column1 = datasource1.split(":")
            column2 = column1
        elif ":" in datasource2:
            datasource2, column2 = datasource2.split(":")
            column1 = column2
        elif (
            "compare_column" in self.project["datasources"][datasource1]
            and "compare_column" in self.project["datasources"][datasource2]
        ):
            column1 = self.project["datasources"][datasource1]["compare_column"]
            column2 = self.project["datasources"][datasource2]["compare_column"]
        elif "compare_column" in self.project["datasources"][datasource1]:
            column1 = self.project["datasources"][datasource1]["compare_column"]
            column2 = column1
        elif "compare_column" in self.project["datasources"][datasource2]:
            column2 = self.project["datasources"][datasource2]["compare_column"]
            column1 = column2
        else:
            raise TulonaMissingPropertyError(
                "Column name must be specified for task: compare-column"
                " either by specifying 'compare_column' property in"
                " at least one of the datasource[project] configs"
                " (check sample tulona-project.yml file for example)"
                " or with '--datasources' command line argument"
                " using one of the following formats"
                " (column name is same for option 3 and 4):-"
                " 1. <datasource1>:<col1>,<datasource2>:<col2>"
                " 2. <datasource1>:<col>,<datasource2>:<col>"
                " 3. <datasource1>:<col>,<datasource2>"
                " 4. <datasource1>,<datasource2>:<col>"
            )

        ds_dict1 = self.project["datasources"][datasource1]
        ds_dict2 = self.project["datasources"][datasource2]

        dbtype1 = self.profile["profiles"][
            extract_profile_name(self.project, datasource1)
        ]["type"]
        dbtype2 = self.profile["profiles"][
            extract_profile_name(self.project, datasource2)
        ]["type"]
        table_name1 = extract_table_name_from_config(config=ds_dict1, dbtype=dbtype1)
        table_name2 = extract_table_name_from_config(config=ds_dict2, dbtype=dbtype2)

        log.debug(f"Extracting data from table: {table_name1}")
        df1 = self.get_column_data(datasource1, table_name1, column1)
        log.debug(f"Extracting data from table: {table_name2}")
        df2 = self.get_column_data(datasource2, table_name2, column2)

        df1 = df1.rename(columns={c: c.lower() for c in df2.columns})
        df2 = df2.rename(columns={c: c.lower() for c in df2.columns})
        column1, column2 = column1.lower(), column2.lower()

        ds1_compressed = datasource1.replace("_", "")
        ds2_compressed = datasource2.replace("_", "")

        df_merge = pd.merge(
            left=df1,
            right=df2,
            left_on=column1,
            right_on=column2,
            how="outer",
            suffixes=["_left_" + ds1_compressed, "_right_" + ds2_compressed],
            validate="one_to_one",
            indicator="presence",
        )
        df_merge = df_merge[df_merge["presence"] != "both"]

        log.debug("Writing comparison result")
        self.write_result(df_merge, ds1_compressed, ds2_compressed)

        end_time = time.time()
        log.info("Finished task: compare-column")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
