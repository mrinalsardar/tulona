import logging
import os
import re
import time
import traceback
from copy import deepcopy
from dataclasses import _MISSING_TYPE, dataclass, fields
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingPrimaryKeyError, TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.task.helper import perform_comparison
from tulona.task.profile import ProfileTask
from tulona.util.database import get_table_primary_keys
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
    "compare_column_composite": False,
}


@dataclass
class CompareRowTask(BaseTask):
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

    def extract_confs(self):
        # TODO: Add support for different names of primary keys in different tables
        # Check if primary key[s] is[are] specified for row comparison
        econf_dict = {}
        primary_keys = []
        econf_dict["ds_names"] = []
        econf_dict["ds_name_compressed_list"] = []
        econf_dict["ds_configs"] = []
        econf_dict["dbtypes"] = []
        econf_dict["table_fqns"] = []
        econf_dict["queries"] = []
        econf_dict["connection_managers"] = []
        econf_dict["exclude_columns_lol"] = []
        for ds_name in self.datasources:
            log.debug(f"Extracting configs for: {ds_name}")
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
            econf_dict["ds_names"].append(ds_name)
            econf_dict["ds_name_compressed_list"].append(ds_name.replace("_", ""))

            ds_config = self.project["datasources"][ds_name]
            econf_dict["ds_configs"].append(ds_config)
            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]
            econf_dict["dbtypes"].append(dbtype)

            if "query" in ds_config and "table" in ds_config:
                raise AttributeError(
                    "Both 'query' and 'table' cannot be specified in datasource config"
                )

            if "query" in ds_config:
                econf_dict["queries"].append(ds_config["query"])
                if "exclude_columns" in ds_config:
                    log.warning(
                        "Attribute 'exclude_columns' doesn't have any effect"
                        " with attribute 'query'"
                    )
                econf_dict["exclude_columns_lol"].append([])
                if "schema" in ds_config:
                    log.warning(
                        "Attribute 'schema' doesn't have any effect"
                        " with attribute 'query'"
                    )
                if "table_fqns" in econf_dict:
                    if len(econf_dict["table_fqns"]) > 0:
                        raise AttributeError(
                            "Same attribute from 'query' and 'table' has to be"
                            " specified in all candidate datasource confgis"
                        )
                    else:
                        econf_dict.pop("table_fqns")

            elif "table" in ds_config:
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

                exclude_columns = (
                    ds_config["exclude_columns"] if "exclude_columns" in ds_config else []
                )
                if isinstance(exclude_columns, str):
                    exclude_columns = [exclude_columns]
                econf_dict["exclude_columns_lol"].append(exclude_columns)

                econf_dict["table_fqns"].append(table_fqn)
                if "queries" in econf_dict:
                    if len(econf_dict["queries"]) > 0:
                        raise AttributeError(
                            "Same attribute from 'query' and 'table' has to be"
                            " specified in all candidate datasource confgis"
                        )
                    else:
                        econf_dict.pop("queries")
            else:
                raise TulonaMissingPropertyError(
                    "Either 'table' for 'query' must be specified"
                    "in datasource config for row comparison."
                )

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)
            econf_dict["connection_managers"].append(conman)

            if "primary_key" in ds_config:
                ds_pk = (
                    (ds_config["primary_key"],)
                    if isinstance(ds_config["primary_key"], str)
                    else tuple(sorted(ds_config["primary_key"]))
                )
                log.debug(f"Provided primary key for datasource {ds_name}: {ds_pk}")
            else:
                log.debug(
                    f"Primary key not provided for datasource {ds_name}"
                    " Tulona will try to extract it from table metadata"
                )
                ds_pk = tuple(get_table_primary_keys(conman.engine, schema, table))
                log.debug(f"Extracted primary key for datasource {ds_name}: {ds_pk}")

            if len(ds_pk) > 0:
                primary_keys.append(ds_pk)

        pk_ci_set = {tuple(map(lambda x: x.lower(), k)) for k in primary_keys}

        if len(primary_keys) == 0:
            raise TulonaMissingPrimaryKeyError(
                "Primary key[s] is[are] not available. Abort!"
            )
        elif len(primary_keys) > 0:
            if len(primary_keys) != len(primary_keys):
                raise TulonaMissingPrimaryKeyError(
                    "If primary key is provided, it must be provided for all datasources"
                )

            if len(pk_ci_set) > 1:
                raise ValueError(
                    "Primary key must be same in all candidate tables for comparison"
                )
            econf_dict["primary_key"] = primary_keys[0]
            log.debug(f"Final primary key: {econf_dict['primary_key']}")
        else:
            raise TulonaMissingPrimaryKeyError(
                f"Primary key for {table_fqn}[{ds_name}] could not be found."
                " Row comparison without primary key has not been implemented yet."
                " Abort!"
            )

        return econf_dict

    def execute(self):
        log.info("------------------------ Starting task: compare-row")
        start_time = time.time()

        if len(self.datasources) != 2:
            raise ValueError("Data comparison needs two data sources.")

        # Config extraction
        econf_dict = self.extract_confs()
        dbtype1, dbtype2 = econf_dict["dbtypes"]
        if "table_fqns" in econf_dict:
            table_fqn1, table_fqn2 = econf_dict["table_fqns"]
        else:
            query1, query2 = econf_dict["queries"]
        exclude_columns1, exclude_columns2 = econf_dict["exclude_columns_lol"]
        conman1, conman2 = econf_dict["connection_managers"]

        # TODO: push column exclusion down to the database/query
        # TODO: We probably don't need to create pk tuple out of pk
        # lists as that is already happening while extracting pks
        primary_key = tuple([k.lower() for k in econf_dict["primary_key"]])
        query_expr = None

        log.info(f"Comparing {self.datasources}")
        num_try = 1 if "queries" in econf_dict else 5
        i = 0
        while i < num_try:
            log.debug(f"Extraction iteration: {i + 1}/{num_try}")

            if "table_fqns" in econf_dict:
                query1 = get_table_data_query(
                    dbtype1, table_fqn1, self.sample_count, query_expr
                )
            if self.sample_count < 51:
                sanitized_query1 = re.sub(r"\(.*\)", "(...)", query1)
                log.debug(f"Executing query: {sanitized_query1}")
            df1 = get_query_output_as_df(connection_manager=conman1, query_text=query1)
            if df1.shape[0] == 0:
                raise ValueError(f"Table {table_fqn1} doesn't have any data")

            df1 = df1.rename(columns={c: c.lower() for c in df1.columns})
            for k in primary_key:
                if k not in df1.columns.tolist():
                    raise ValueError(f"Primary key {k} not present in {table_fqn1}")

            # Exclude columns
            if len(exclude_columns1) > 0:
                log.debug(f"Excluding columns from {table_fqn1}: {exclude_columns1}")
                df1 = apply_column_exclusion(
                    df1, primary_key, exclude_columns1, table_fqn1
                )
            if "table_fqns" in econf_dict:
                query2 = get_table_data_query(
                    dbtype2,
                    table_fqn2,
                    self.sample_count,
                    query_expr=build_filter_query_expression(df1, primary_key),
                )
            if self.sample_count < 51:
                sanitized_query2 = re.sub(r"\(.*\)", "(...)", query2)
                log.debug(f"Executing query: {sanitized_query2}")

            df2 = get_query_output_as_df(connection_manager=conman2, query_text=query2)
            df2 = df2.rename(columns={c: c.lower() for c in df2.columns})

            for k in primary_key:
                if k not in df2.columns.tolist():
                    raise ValueError(f"Primary key {k} not present in {table_fqn2}")

            # Exclude columns
            if len(exclude_columns2) > 0:
                log.debug(f"Excluding columns from {table_fqn2}: {exclude_columns2}")
                df2 = apply_column_exclusion(
                    df2, primary_key, exclude_columns2, table_fqn2
                )

            if df2.shape[0] > 0:
                for k in primary_key:
                    df1 = df1[df1[k].isin(df2[k].tolist())]
                row_data_list = [df1, df2]
                break
            else:
                query_expr = build_filter_query_expression(
                    df1, primary_key, positive=False
                )

            i += 1

        if df2.shape[0] == 0:
            raise ValueError(
                f"Could not find common rows between {table_fqn1} and {table_fqn2}"
            )

        log.debug(
            f"Preparing row comparison for: {econf_dict['ds_name_compressed_list']}"
        )
        df_row_comp = perform_comparison(
            ds_compressed_names=econf_dict["ds_name_compressed_list"],
            dataframes=row_data_list,
            on=primary_key,
        )
        log.debug(f"Prepared comparison for {df_row_comp.shape[0]} rows")

        log.debug(f"Writing comparison result into: {self.outfile_fqn}")
        # TODO: Remove it as it is already happening in perform_comparison
        # Moving key columns to the beginning
        new_columns = list(primary_key) + [
            col for col in df_row_comp if col not in primary_key
        ]
        df_row_comp = df_row_comp[new_columns]

        _ = create_dir_if_not_exist(self.outfile_fqn.parent)
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

        exec_time = time.time() - start_time
        log.info(f"Finished task: compare-row in {exec_time:.2f} seconds")


@dataclass
class CompareColumnTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]
    composite: bool = DEFAULT_VALUES["compare_column_composite"]

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
                columns = ds_config["compare_column"]
                columns = [columns] if isinstance(columns, str) else columns
                compare_columns.append(columns)
                log.debug(f"Column[s] to compare: {columns}")
            else:
                raise TulonaMissingPropertyError(
                    "Property 'compare_column' must be specified"
                    " in project config for column comparison"
                )

            dbtype = self.profile["profiles"][
                extract_profile_name(self.project, ds_name)
            ]["type"]
            log.debug(f"Database type: {dbtype}")

            log.debug(f"Acquiring connection to the database of: {ds_name}")
            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            if "query" in ds_config and "table" in ds_config:
                raise AttributeError(
                    "Both 'query' and 'table' cannot be specified in datasource config"
                )
            if "query" in ds_config:
                query = ds_config["query"]
                log.debug(f"Executing query: {query}")
                df = get_query_output_as_df(connection_manager=conman, query_text=query)
            elif "table" in ds_config:
                # MySQL doesn't have logical database
                if "database" in ds_config and dbtype.lower() != "mysql":
                    database = ds_config["database"]
                else:
                    database = None
                if "schema" in ds_config:
                    schema = ds_config["schema"]

                table = ds_config["table"]
                table_fqn = get_table_fqn(database, schema, table)
                log.debug(f"Table FQN: {table_fqn}")
                query = get_column_query(table_fqn, columns)
                try:
                    log.debug(f"Trying unquoted column names: {columns}")
                    log.debug(f"Executing query: {query}")
                    df = get_query_output_as_df(
                        connection_manager=conman, query_text=query
                    )
                except Exception as exc:
                    log.warning(f"Failed with error: {exc}")
                    log.debug(f'Trying quoted column names: "{columns}"')
                    query = get_column_query(table_fqn, columns, quoted=True)
                    log.debug(f"Executing query: {query}")
                    df = get_query_output_as_df(
                        connection_manager=conman, query_text=query
                    )
            else:
                raise TulonaMissingPropertyError(
                    "Either 'table' for 'query' must be specified"
                    "in datasource config for row comparison."
                )

            if df.shape[0] == 0:
                raise ValueError("Query didn't result into any data")

            log.debug(f"Extracted {df.shape[0]} records as query result")

            df = df.rename(columns={c: c.lower() for c in df.columns})
            column_df_list.append(df)

        compare_columns = {
            tuple(map(lambda c: c.lower(), clist)) for clist in compare_columns
        }
        if len(compare_columns) > 1:
            raise ValueError(
                "Column comparison works only when the column name is same for all data sources"
                " (not case sensitive)"
                " and they have to be specified in the same order"
                " in the config file for all data sources"
            )
        compare_columns = compare_columns.pop()
        log.debug(f"Final list of columns for comparison: {compare_columns}")

        output_dataframes = dict()
        if self.composite:
            log.debug(f"Performing composite comparison for: {compare_columns}")
            df_comp = perform_comparison(
                ds_compressed_names=ds_compressed_names,
                dataframes=column_df_list,
                on=compare_columns,
                how="outer",
                indicator="presence",
                validate="one_to_one",
            )
            df_comp = df_comp[df_comp["presence"] != "both"]
            df_comp["presence"] = df_comp["presence"].map(
                {
                    "left_only": ds_compressed_names[0],
                    "right_only": ds_compressed_names[1],
                }
            )
            log.debug(f"Found {df_comp.shape[0]} mismatches all sides combined")
            output_dataframes["-".join(compare_columns)] = df_comp
        else:
            for c in compare_columns:
                log.debug(f"Performing comparison for: {c}")
                column_df_list_unique = [
                    pd.DataFrame(df[c].drop_duplicates()) for df in column_df_list
                ]
                df_comp = perform_comparison(
                    ds_compressed_names=ds_compressed_names,
                    dataframes=column_df_list_unique,
                    on=c,
                    how="outer",
                    indicator="presence",
                    validate="one_to_one",
                )
                df_comp = df_comp[df_comp["presence"] != "both"]
                df_comp["presence"] = df_comp["presence"].map(
                    {
                        "left_only": ds_compressed_names[0],
                        "right_only": ds_compressed_names[1],
                    }
                )
                log.debug(f"Found {df_comp.shape[0]} mismatches all sides combined")
                output_dataframes[c] = df_comp

        log.debug(f"Writing output into: {self.outfile_fqn}")
        _ = create_dir_if_not_exist(self.outfile_fqn.parent)
        for sheet, df in output_dataframes.items():
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                df.to_excel(
                    writer, sheet_name=f"Column Comparison-> {sheet}", index=False
                )

        exec_time = time.time() - start_time
        log.info(f"Finished task: compare-column in {exec_time:.2f} seconds")


@dataclass
class CompareTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]
    sample_count: int = DEFAULT_VALUES["sample_count"]
    composite: bool = DEFAULT_VALUES["compare_column_composite"]

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
        except Exception:
            log.error(f"Profiling failed with error: {traceback.format_exc()}")

        # Row comparison
        primary_key = None
        cdt = CompareRowTask(
            profile=self.profile,
            project=self.project,
            runtime=self.runtime,
            datasources=self.datasources,
            outfile_fqn=self.outfile_fqn,
            sample_count=self.sample_count,
        )
        try:
            primary_key = cdt.extract_confs()["primary_key"]
            cdt.execute()
        except Exception:
            log.error(f"Row comparison failed with error: {traceback.format_exc()}")

        # Column comparison
        project_copy = deepcopy(self.project)
        for ds in project_copy["datasources"]:
            if "compare_column" not in ds and primary_key:
                project_copy["datasources"][ds]["compare_column"] = primary_key
        try:
            CompareColumnTask(
                profile=self.profile,
                project=project_copy,
                runtime=self.runtime,
                datasources=self.datasources,
                outfile_fqn=self.outfile_fqn,
                composite=self.composite,
            ).execute()
        except Exception:
            log.error(f"Column comparison failed with error: {traceback.format_exc()}")

        exec_time = time.time() - start_time
        log.info(
            "Finished task: compare[profile, compare-row, compare-column]"
            f" in {exec_time:.2f} seconds"
        )
