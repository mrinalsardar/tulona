import logging
import os
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingPrimaryKeyError, TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.task.helper import perform_comparison
from tulona.task.profile import ProfileTask
from tulona.util.dataframe import apply_column_exclusion
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.sql import (
    build_filter_query_expression,
    get_column_query,
    get_query_output_as_df,
    get_table_data_query,
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
    outfile_fqn: Union[Path, str]
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
        log.info("------------------------ Starting task: compare-data")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Data comparison needs two data sources.")

        log.info(f"Comparing {self.datasources}")

        # TODO: Add support of composite primary key
        # TODO: Add support for different names of primary keys in different tables
        # Check if primary key[s] is[are] specified for row comparison
        primary_keys = set()
        ds_names = []
        ds_name_compressed_list = []
        ds_configs = []
        dbtypes = []
        table_fqns = []
        connection_managers = []
        exclude_columns_lol = []
        for ds_name in self.datasources:
            log.debug(f"Extracting configs for: {ds_name}")
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
            ds_names.append(ds_name)
            ds_name_compressed_list.append(ds_name.replace("_", ""))

            ds_config = self.project["datasources"][ds_name]
            ds_configs.append(ds_config)
            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]
            dbtypes.append(dbtype)

            # MySQL doesn't have logical database
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = None
            schema = ds_config["schema"]
            table = ds_config["table"]

            table_fqn = get_table_fqn(
                database,
                schema,
                table,
            )
            table_fqns.append(table_fqn)

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(
                self.profile, self.project, ds_name
            )
            connection_managers.append(
                self.get_connection_manager(conn_profile=connection_profile)
            )

            exclude_columns = (
                ds_config["exclude_columns"] if "exclude_columns" in ds_config else []
            )
            if isinstance(exclude_columns, str):
                exclude_columns = [exclude_columns]
            exclude_columns_lol.append(exclude_columns)

            if "primary_key" in ds_config:
                if (
                    isinstance(ds_config["primary_key"], list)
                    and len(ds_config["primary_key"]) > 1
                ):
                    raise ValueError("Composite primary key is not supported yet")
                primary_keys = primary_keys.union({ds_config["primary_key"]})

        if len(primary_keys) == 0:
            raise TulonaMissingPrimaryKeyError(
                "Primary key must be provided with at least one of the data source config"
            )

        if len(primary_keys) > 1:
            raise ValueError(
                "Primary key column name has to be same in all candidate tables for comparison"
            )
        primary_key = primary_keys.pop()

        # Config extraction
        dbtype1, dbtype2 = dbtypes
        table_fqn1, table_fqn2 = table_fqns
        conman1, conman2 = connection_managers
        exclude_columns1, exclude_columns2 = exclude_columns_lol

        # TODO: push column exclusion down to the database/query
        primary_key = primary_key.lower()
        query_expr = None

        i = 0
        while i < 5:
            log.debug(f"Extraction iteration: {i + 1}/5")

            query1 = get_table_data_query(
                dbtype1, table_fqn1, self.sample_count, query_expr
            )
            if self.sample_count < 51:
                log.debug(f"Executing query: {query1}")
            df1 = get_query_output_as_df(connection_manager=conman1, query_text=query1)
            if df1.shape[0] == 0:
                raise ValueError(f"Table {table_fqn1} doesn't have any data")

            df1 = df1.rename(columns={c: c.lower() for c in df1.columns})
            if primary_key not in df1.columns.tolist():
                raise ValueError(f"Primary key {primary_key} not present in {table_fqn2}")

            # Exclude columns
            log.debug(f"Excluding columns from {table_fqn1}")
            if len(exclude_columns1):
                df1 = apply_column_exclusion(
                    df1, primary_key, exclude_columns1, table_fqn1
                )

            query2 = get_table_data_query(
                dbtype2,
                table_fqn2,
                self.sample_count,
                query_expr=build_filter_query_expression(df1, primary_key),
            )
            if self.sample_count < 51:
                log.debug(f"Executing query: {query2}")
            df2 = get_query_output_as_df(connection_manager=conman2, query_text=query2)
            df2 = df2.rename(columns={c: c.lower() for c in df2.columns})

            if primary_key not in df2.columns.tolist():
                raise ValueError(f"Primary key {primary_key} not present in {table_fqn2}")

            # Exclude columns
            log.debug(f"Excluding columns from {table_fqn2}")
            if len(exclude_columns2):
                df2 = apply_column_exclusion(
                    df2, primary_key, exclude_columns2, table_fqn2
                )

            if df2.shape[0] > 0:
                df1 = df1[df1[primary_key].isin(df2[primary_key].tolist())]
                row_data_list = [df1, df2]
                break
            else:
                query_expr = build_filter_query_expression(
                    df1, primary_key, positive=False
                )

            i += 1

        if df2.shape[0] == 0:
            raise ValueError(
                f"Could not find common data between {table_fqn1} and {table_fqn2}"
            )

        log.debug("Preparing row comparison")
        df_row_comp = perform_comparison(
            ds_name_compressed_list, row_data_list, primary_key
        )
        log.debug(f"Prepared comparison for {df_row_comp.shape[0]} rows")

        log.debug(f"Writing comparison result into: {self.outfile_fqn}")
        _ = create_dir_if_not_exist(self.project["outdir"])
        with pd.ExcelWriter(
            self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
        ) as writer:
            df_row_comp.to_excel(writer, sheet_name="Row Comparison", index=False)

        log.debug("Highlighting mismtach cells")
        highlight_mismatch_cells(
            excel_file=self.outfile_fqn,
            sheet="Row Comparison",
            num_ds=len(self.datasources),
            skip_columns=primary_key,
        )

        end_time = time.time()
        log.info("------------------------ Finished task: compare-data")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")


@dataclass
class CompareColumnTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]

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
        log.info("------------------------ Starting task: compare-column")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Comparison works between two entities, not more, not less.")

        ds_compressed_names = []
        compare_columns = []
        column_df_list = []
        for ds_name in self.datasources:
            log.info(f"Processing data source {ds_name}")
            ds_compressed_names.append(ds_name.replace("_", ""))
            ds_config = self.project["datasources"][ds_name]

            if "compare_column" in ds_config:
                column = ds_config["compare_column"]
                compare_columns.append(column)
            else:
                raise TulonaMissingPropertyError(
                    "Property 'compare_column' must be specified for column comparison"
                )
            log.debug(f"Extracting data for column {column}")

            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]
            log.debug(f"Database type: {dbtype}")

            # MySQL doesn't have logical database
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = None
            schema = ds_config["schema"]
            table = ds_config["table"]
            table_fqn = get_table_fqn(database, schema, table)
            log.debug(f"Table FQN: {table_fqn}")

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(
                self.profile, self.project, ds_name
            )
            conman = self.get_connection_manager(conn_profile=connection_profile)

            query = get_column_query(table_fqn, column)
            try:
                log.debug(f"Trying unquoted column name: {column}")
                log.debug(f"Executing query: {query}")
                df = get_query_output_as_df(connection_manager=conman, query_text=query)
            except Exception as exp:
                log.warning(f"Failed with error: {exp}")
                log.debug(f'Trying quoted column name: "{column}"')
                query = get_column_query(table_fqn, column, quoted=True)
                log.debug(f"Executing query: {query}")
                df = get_query_output_as_df(connection_manager=conman, query_text=query)

            if df.shape[0] == 0:
                raise ValueError(f"Table {table_fqn} doesn't have any data")

            log.debug(f"Found {df.shape[0]} records in {table_fqn}")

            df = df.rename(columns={c: c.lower() for c in df.columns})
            column_df_list.append(df)

        compare_columns = {c.lower() for c in compare_columns}
        if len(compare_columns) > 1:
            raise ValueError(
                "Column comparison works only when the column name is same for both data source"
                "(not case sensitive)"
            )

        log.debug("Perform comparison")
        df_comp = perform_comparison(
            ds_compressed_names=ds_compressed_names,
            dataframes=column_df_list,
            on=compare_columns.pop(),
            how="outer",
            indicator="presence",
            validate="one_to_one",
        )
        df_comp = df_comp[df_comp["presence"] != "both"]
        df_comp["presence"] = df_comp["presence"].map(
            {"left_only": ds_compressed_names[0], "right_only": ds_compressed_names[1]}
        )
        log.debug(f"Found {df_comp.shape[0]} mismatches both side combined")

        log.debug(f"Writing output into: {self.outfile_fqn}")
        _ = create_dir_if_not_exist(self.project["outdir"])
        with pd.ExcelWriter(
            self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
        ) as writer:
            df_comp.to_excel(writer, sheet_name="Column Comparison", index=False)

        end_time = time.time()
        log.info("------------------------ Finished task: compare-column")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")


@dataclass
class CompareTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]
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
        log.info("------------------------ Starting task: compare")
        start_time = time.time()

        # Metadata comparison
        try:
            ProfileTask(
                profile=self.profile,
                project=self.project,
                runtime=self.runtime,
                datasources=self.datasources,
                outfile_fqn=self.outfile_fqn,
                compare=True,
            ).execute()
        except Exception as exc:
            log.error(f"Profiling failed with error: {exc}")

        # Row comparison
        try:
            CompareDataTask(
                profile=self.profile,
                project=self.project,
                runtime=self.runtime,
                datasources=self.datasources,
                outfile_fqn=self.outfile_fqn,
                sample_count=self.sample_count,
            ).execute()
        except Exception as exc:
            log.error(f"Row comparison failed with error: {exc}")

        # Column comparison
        try:
            CompareColumnTask(
                profile=self.profile,
                project=self.project,
                runtime=self.runtime,
                datasources=self.datasources,
                outfile_fqn=self.outfile_fqn,
            ).execute()
        except Exception as exc:
            log.error(f"Column comparison failed with error: {exc}")

        end_time = time.time()
        log.info("------------------------ Finished task: compare")
        log.info(
            f"Total time taken [profile, compare-data, compare-column]: {(end_time - start_time):.2f} seconds"
        )
