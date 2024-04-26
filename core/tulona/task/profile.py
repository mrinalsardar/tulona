import logging
import os
import time
from dataclasses import _MISSING_TYPE, dataclass, fields
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

from tulona.config.runtime import RunConfig
from tulona.task.base import BaseTask
from tulona.task.helper import create_profile, perform_comparison
from tulona.util.excel import highlight_mismatch_cells
from tulona.util.filesystem import create_dir_if_not_exist
from tulona.util.profiles import extract_profile_name, get_connection_profile

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    "compare_profiles": False,
}


@dataclass
class ProfileTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]
    compare: bool = DEFAULT_VALUES["compare_profiles"]

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

        log.info("------------------------ Starting task: profile")
        start_time = time.time()

        df_collection = []
        ds_name_compressed_list = []
        for ds_name in self.datasources:
            log.debug(f"Extracting configs for: {ds_name}")
            # Extract data source name from datasource:column combination
            ds_name = ds_name.split(":")[0]
            ds_name_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_name_compressed)

            ds_config = self.project["datasources"][ds_name]

            if "query" in ds_config:
                raise AttributeError(
                    "Profiling only works with tables, not with queries"
                    f" Check your datasource config for: {ds_name}"
                )

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
            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            log.info(f"Profiling {ds_name}")
            metrics = [
                "min",
                "max",
                "avg",
                "count",
                "distinct_count",
            ]
            df = create_profile(database, schema, table, metrics, conman)
            df_collection.append(df)

        _ = create_dir_if_not_exist(self.outfile_fqn.parent)
        if self.compare:
            log.debug("Preparing metadata comparison")
            df_merge = perform_comparison(
                ds_name_compressed_list, df_collection, "column_name"
            )
            log.debug(f"Calculated comparison for {df_merge.shape[0]} columns")

            log.debug(f"Writing results into file: {self.outfile_fqn}")
            primary_key_col = df_merge.pop("column_name")
            df_merge.insert(loc=0, column="column_name", value=primary_key_col)
            with pd.ExcelWriter(
                self.outfile_fqn, mode="a" if os.path.exists(self.outfile_fqn) else "w"
            ) as writer:
                df_merge.to_excel(writer, sheet_name="Metadata Comparison", index=False)

            log.debug("Highlighting mismtach cells")
            highlight_mismatch_cells(
                excel_file=self.outfile_fqn,
                sheet="Metadata Comparison",
                num_ds=len(ds_name_compressed_list),
                skip_columns="column_name",
            )
        else:
            log.debug(f"Writing results into file: {self.outfile_fqn}")
            with pd.ExcelWriter(self.outfile_fqn) as writer:
                for ds_name, df in zip(ds_name_compressed_list, df_collection):
                    primary_key_col = df.pop("column_name")
                    df.insert(loc=0, column="column_name", value=primary_key_col)
                    df.to_excel(writer, sheet_name=f"{ds_name} Metadata", index=False)

        end_time = time.time()
        log.info("------------------------ Finished task: profile")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
