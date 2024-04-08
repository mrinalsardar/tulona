import logging
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from typing import Dict, List

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingPrimaryKeyError, TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.task.helper import create_profile, extract_rows, perform_comparison
from tulona.util.dataframe import apply_column_exclusion
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import get_outfile_fqn
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.project import extract_table_name_from_config
from tulona.util.sql import (
    build_filter_query_expression,
    get_column_query,
    get_query_output_as_df,
    get_sample_row_query,
    get_table_fqn,
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
        table_name1 = get_table_fqn(
            ds_dict1["database"] if dbtype1 != "mysql" else None,
            ds_dict1["schema"],
            ds_dict1["table"],
        )
        table_name2 = get_table_fqn(
            ds_dict2["database"] if dbtype2 != "mysql" else None,
            ds_dict2["schema"],
            ds_dict2["table"],
        )

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
            df1 = apply_column_exclusion(
                df1, ds_dict1["primary_key"], ds_dict1["exclude_columns"], table_name1
            )
        if "exclude_columns" in ds_dict2:
            df2 = apply_column_exclusion(
                df2, ds_dict2["primary_key"], ds_dict2["exclude_columns"], table_name2
            )

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

        ds_name_compressed_list = [ds1_compressed, ds2_compressed]
        outfile_fqn = get_outfile_fqn(
            self.project["outdir"], ds_name_compressed_list, "data_comparison"
        )
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
            suffixes=("_left_" + ds1_compressed, "_right_" + ds2_compressed),
            validate="one_to_one",
            indicator="presence",
        )
        df_merge = df_merge[df_merge["presence"] != "both"]

        ds_name_compressed_list = [ds1_compressed, ds2_compressed]
        outfile_fqn = get_outfile_fqn(
            self.project["outdir"], ds_name_compressed_list, "column_comparison"
        )
        log.debug(f"Writing output into: {outfile_fqn}")
        df_merge.to_excel(outfile_fqn, sheet_name="Column Comparison", index=False)

        end_time = time.time()
        log.info("Finished task: compare-column")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")


@dataclass
class CompareTask(BaseTask):
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

    def execute(self):
        log.info("Starting task: compare")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison needs two data sources.")

        # ------- Row comparison: different beast | against DRY principle
        # TODO: Add support of composite primary key
        # TODO: Add support for different names of primary keys in different tables
        # Check if primary key[s] is[are] specified for row comparison
        primary_keys = set()
        for ds_name in self.datasources:
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
            ds_config = self.project["datasources"][ds_name]
            if "primary_key" in ds_config:
                if (
                    isinstance(ds_config["primary_key"], list)
                    and len(ds_config["primary_key"]) > 1
                ):
                    raise ValueError("Composite primary key is not supported yet")
                primary_keys = primary_keys.union({ds_config["primary_key"]})

        if len(primary_keys) == 0:
            raise ValueError(
                "Primary key must be provided with at least one of the data source config"
            )

        if len(primary_keys) > 1:
            raise ValueError(
                "Primary key column name has to be same in all candidate tables for comparison"
            )
        primary_key = primary_keys.pop()

        # Config extraction
        ds1_name = self.datasources[0].split(":")[0]
        ds1_config = self.project["datasources"][ds1_name]
        dbtype1 = self.profile["profiles"][extract_profile_name(self.project, ds1_name)][
            "type"
        ]
        table_fqn1 = get_table_fqn(
            ds1_config["database"] if "database" in ds1_config else None,
            ds1_config["schema"],
            ds1_config["table"],
        )
        connection_profile1 = get_connection_profile(self.profile, self.project, ds1_name)
        conman1 = self.get_connection_manager(conn_profile=connection_profile1)
        exclude_columns1 = (
            ds1_config["exclude_columns"] if "exclude_columns" in ds1_config else []
        )
        if isinstance(exclude_columns1, str):
            exclude_columns1 = [exclude_columns1]

        ds2_name = self.datasources[1].split(":")[0]
        ds2_config = self.project["datasources"][ds2_name]
        dbtype2 = self.profile["profiles"][extract_profile_name(self.project, ds2_name)][
            "type"
        ]
        table_fqn2 = get_table_fqn(
            ds2_config["database"] if "database" in ds2_config else None,
            ds2_config["schema"],
            ds2_config["table"],
        )
        connection_profile2 = get_connection_profile(self.profile, self.project, ds2_name)
        conman2 = self.get_connection_manager(conn_profile=connection_profile2)
        exclude_columns2 = (
            ds2_config["exclude_columns"] if "exclude_columns" in ds2_config else []
        )
        if isinstance(exclude_columns2, str):
            exclude_columns2 = [exclude_columns2]

        log.info("Extracting row data")
        row_data_list = extract_rows(
            dbtype1=dbtype1,
            table_fqn1=table_fqn1,
            conman1=conman1,
            exclude_columns1=exclude_columns1,
            dbtype2=dbtype2,
            table_fqn2=table_fqn2,
            conman2=conman2,
            exclude_columns2=exclude_columns2,
            primary_key=primary_key,
            sample_count=self.sample_count,
        )

        # --------------- Data collection from sources
        ds_name_compressed_list = []
        profile_list = []
        # column_data_list = []
        for ds_name in self.datasources:
            log.debug(f"Extracting configs for: {ds_name}")
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
            ds_name_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_name_compressed)

            ds_config = self.project["datasources"][ds_name]
            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]

            # MySQL doesn't have logical database
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = None
            schema = ds_config["schema"]
            table = ds_config["table"]

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(
                self.profile, self.project, ds_name
            )
            conman = self.get_connection_manager(conn_profile=connection_profile)

            # Profile data
            log.info(f"Extracting profile data for {ds_name}")
            metrics = [
                "min",
                "max",
                "avg",
                "count",
                "distinct_count",
            ]
            df = create_profile(database, schema, table, metrics, conman)
            profile_list.append(df)

            # Column data

        # --------------- Comparison
        comparisons = {}

        # Row data comparison
        log.debug("Preparing row data comparison")
        df_row_comp = perform_comparison(
            ds_name_compressed_list, row_data_list, primary_key
        )
        comparisons["Row Comparison"] = {
            "primary_key": primary_key.lower(),
            "data": df_row_comp,
            "num_sources": len(row_data_list),
        }
        log.debug(f"Prepared comparison for {df_row_comp.shape[0]} rows")

        # Profile comparison
        log.debug("Preparing metadata comparison")
        df_profiles = perform_comparison(
            ds_name_compressed_list, profile_list, "column_name"
        )
        comparisons["Metadata Comparison"] = {
            "primary_key": "column_name",
            "data": df_profiles,
            "num_sources": len(ds_name_compressed_list),
        }
        log.debug(f"Prepared comparison for {df_profiles.shape[0]} columns")

        # Column comparison

        outfile_fqn = get_outfile_fqn(
            self.project["outdir"], ds_name_compressed_list, "comparison"
        )
        log.debug(f"Writing results into file: {outfile_fqn}")
        with pd.ExcelWriter(outfile_fqn) as writer:
            for sheet, content in comparisons.items():
                primary_key_series = content["data"].pop(content["primary_key"])
                content["data"].insert(
                    loc=0, column=content["primary_key"], value=primary_key_series
                )
                content["data"].to_excel(writer, sheet_name=sheet, index=False)

        log.debug("Highlighting mismtach cells")
        for sheet, content in comparisons.items():
            highlight_mismatch_cells(
                excel_file=outfile_fqn,
                sheet=sheet,
                num_ds=content["num_sources"],
                skip_columns=content["primary_key"],
            )

        end_time = time.time()
        log.info("Finished task: compare")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
