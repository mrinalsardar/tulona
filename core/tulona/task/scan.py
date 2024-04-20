import logging
import os
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Union

from tulona.config.runtime import RunConfig
from tulona.task.base import BaseTask
from tulona.task.compare import CompareTask
from tulona.task.helper import perform_comparison
from tulona.util.excel import dataframes_into_excel
from tulona.util.filesystem import create_dir_if_not_exist, get_outfile_fqn
from tulona.util.profiles import extract_profile_name, get_connection_profile
from tulona.util.sql import get_query_output_as_df

log = logging.getLogger(__name__)

DEFAULT_VALUES = {
    "compare_scans": False,
    "sample_count": 20,
    "compare_column_composite": False,
}
META_EXCLUSION = {
    "schemas": ["INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA"],
}


@dataclass
class ScanTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig
    datasources: List[str]
    outfile_fqn: Union[Path, str]
    compare: bool = DEFAULT_VALUES["compare_scans"]
    sample_count: int = DEFAULT_VALUES["sample_count"]
    composite: bool = DEFAULT_VALUES["compare_column_composite"]

    def execute(self):
        log.info(f"Starting task: scan{' --compare' if self.compare else ''}")
        log.debug(f"Datasource: {self.datasources}")
        log.debug(f"Compare: {self.compare}")
        log.debug(f"Output file: {self.outfile_fqn}")
        start_time = time.time()

        scan_result = {}
        ds_name_compressed_list = []
        connection_profile_names = []
        primary_keys = []
        for ds_name in self.datasources:
            log.info(f"Processing datasource {ds_name}")
            ds_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_compressed)
            ds_config = self.project["datasources"][ds_name]
            scan_result[ds_name] = {}
            scan_result[ds_name]["database"] = {}
            scan_result[ds_name]["schema"] = {}
            if "primary_key" in ds_config:
                primary_keys.append(ds_config["primary_key"])

            connection_profile_name = extract_profile_name(self.project, ds_name)
            connection_profile_names.append(connection_profile_name)
            dbtype = self.profile["profiles"][connection_profile_name]["type"]
            log.debug(
                f"Connection profile: {connection_profile_name} | Database type: {dbtype}"
            )
            scan_result[ds_name]["dbtype"] = dbtype

            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            # MySQL doesn't have logical database
            if "database" in ds_config and dbtype.lower() != "mysql":
                database = ds_config["database"]
            else:
                database = "def"

            # Create output directory
            _ = create_dir_if_not_exist(self.project["outdir"])

            # Database scan
            log.debug(f"Performing database scan for: {database}")
            if "schema" in ds_config:
                schemata_query = f"""
                select
                    *
                from
                    information_schema.schemata
                where
                    upper(catalog_name) = '{database.upper()}'
                    and upper(schema_name) = '{ds_config["schema"].upper()}'
                """
            else:
                schemata_query = f"""
                select
                    *
                from
                    information_schema.schemata
                where
                    upper(catalog_name) = '{database.upper()}'
                    and upper(schema_name) not in (
                        '{"', '".join(META_EXCLUSION['schemas'])}'
                    )
                """
            log.debug(f"Executing query: {schemata_query}")
            dbextract_df = get_query_output_as_df(
                connection_manager=conman, query_text=schemata_query
            )
            log.debug(f"Number of schemas found: {dbextract_df.shape[0]}")

            dbextract_df = dbextract_df.rename(
                columns={c: c.lower() for c in dbextract_df.columns}
            )

            if not self.compare:
                # Writing scan result
                dbscan_outfile_fqn = get_outfile_fqn(
                    outdir=self.project["outdir"],
                    ds_list=[ds_compressed],
                    infix="scan",
                )
                log.debug(f"Writing db scan result into: {dbscan_outfile_fqn}")
                dataframes_into_excel(
                    sheet_df_map={database: dbextract_df},
                    outfile_fqn=dbscan_outfile_fqn,
                    mode="a" if os.path.exists(dbscan_outfile_fqn) else "w",
                )

            scan_result[ds_name]["database"][database] = dbextract_df

            # Schema scan
            schema_list = dbextract_df["schema_name"].tolist()
            for schema in schema_list:
                log.debug(f"Performing schema scan for: {database}.{schema}")
                tables_query = f"""
                select
                    *
                from
                    information_schema.tables
                where
                    upper(table_catalog) = '{database.upper()}'
                    and upper(table_schema) = '{schema.upper()}'
                """
                log.debug(f"Executing query: {tables_query}")
                schemaextract_df = get_query_output_as_df(
                    connection_manager=conman, query_text=tables_query
                )
                log.debug(f"Number of tables found: {schemaextract_df.shape[0]}")
                schemaextract_df = schemaextract_df.rename(
                    columns={c: c.lower() for c in schemaextract_df.columns}
                )

                if not self.compare:
                    # Writing scan result
                    schemascan_outfile_fqn = get_outfile_fqn(
                        outdir=self.project["outdir"],
                        ds_list=[f"{ds_compressed}_{schema}"],
                        infix="scan",
                    )
                    log.debug(
                        f"Writing schema scan result into: {schemascan_outfile_fqn}"
                    )
                    dataframes_into_excel(
                        sheet_df_map={schema: schemaextract_df},
                        outfile_fqn=schemascan_outfile_fqn,
                        mode="a" if os.path.exists(schemascan_outfile_fqn) else "w",
                    )
                scan_result[ds_name]["schema"][f"{database}.{schema}"] = schemaextract_df
                # write_map[f"{ds_compressed}_{schema}"] = schemaextract_df

        # Handle primary keys for table comparison
        table_primary_key = list(set(primary_keys))
        if not table_primary_key or len(primary_keys) != len(ds_name_compressed_list):
            log.warning(
                "Primary key[s] is[are] not specified for any/all datasources up for comparison"
                " face to face otherwise table comparison won't work."
                "In future tulona will try to extract primary key from table metadata"
                "but not yet."
            )
            table_primary_key = None
        if table_primary_key and len(table_primary_key) > 1:
            log.warning(
                "Primary key[s] must be same for all datasources up for comparison face to face"
                "otherwise table comparison won't work"
            )
            table_primary_key = None

        if self.compare:
            log.debug("Preparing metadata comparison")

            # Compare database extracts
            databases = [list(scan_result[k]["database"].keys())[0] for k in scan_result]
            db_frames = [
                list(scan_result[k]["database"].values())[0] for k in scan_result
            ]
            dbtypes = [scan_result[k]["dbtype"] for k in scan_result]
            log.debug(f"Comparing databases: {' vs '.join(databases)}")
            db_comp = perform_comparison(
                databases,
                db_frames,
                on="schema_name",
                how="outer",
                suffixes=ds_name_compressed_list,
                indicator="presence",
            )

            # Writing database comparison result
            dbcomp_outfile_fqn = get_outfile_fqn(
                outdir=self.project["outdir"],
                ds_list=databases,
                infix="scan",
            )
            log.debug(f"Writing db scan comparison result into: {dbcomp_outfile_fqn}")
            dataframes_into_excel(
                sheet_df_map={f"db_{'|'.join(databases)}": db_comp},
                outfile_fqn=dbcomp_outfile_fqn,
                mode="a" if os.path.exists(dbcomp_outfile_fqn) else "w",
            )

            # Compare schema extracts
            common_schemas = db_comp[db_comp["presence"] == "both"][
                "schema_name"
            ].tolist()
            log.debug(f"Number of common schemas found: {len(common_schemas)}")

            for sc in common_schemas:
                log.debug(f"Comparing schema: {sc}")
                schema_fqns = [f"{db}.{sc}" for db in databases]
                schema_compressed = [
                    sf.replace(".", "").replace("_", "") for sf in schema_fqns
                ]
                schema_frames = [
                    scan_result[ds_name]["schema"][f"{db}.{sc}"]
                    for ds_name, db in zip(self.datasources, databases)
                ]

                schema_comp = perform_comparison(
                    schema_fqns,
                    schema_frames,
                    on="table_name",
                    how="outer",
                    suffixes=schema_compressed,
                    indicator="presence",
                )

                # Writing schema comparison result
                schemacomp_outfile_fqn = get_outfile_fqn(
                    outdir=self.project["outdir"],
                    ds_list=schema_compressed,
                    infix="scan",
                )
                log.debug(
                    f"Writing schema scan comparison result into: {schemacomp_outfile_fqn}"
                )
                dataframes_into_excel(
                    sheet_df_map={"|".join(schema_compressed): schema_comp},
                    outfile_fqn=schemacomp_outfile_fqn,
                    mode="a" if os.path.exists(schemacomp_outfile_fqn) else "w",
                )

                # Compare tables
                if table_primary_key:
                    common_tables = schema_comp[schema_comp["presence"] == "both"][
                        "table_name"
                    ].tolist()
                    log.debug(
                        f"Number of common_tables found in schema {sc}: {len(common_tables)}"
                    )

                    sc_comp = sc.replace("_", "")
                    dynamic_project_config = deepcopy(self.project["datasources"])
                    dynamic_project_config["datasources"] = {}
                    if "source_map" in dynamic_project_config:
                        dynamic_project_config.pop("source_map")
                    for table in common_tables:
                        log.debug(f"Comparing table: {sc}.{table}")

                        source_map_item = []
                        for ds_name, db, typ, cpn in zip(
                            ds_name_compressed_list,
                            databases,
                            dbtypes,
                            connection_profile_names,
                        ):
                            table_ds_config = {
                                "connection_profile": cpn,
                                "schema": sc,
                                "table": table,
                                "primary_key": table_primary_key,
                                "compare_column": table_primary_key,
                            }
                            if typ != "mysql":
                                table_ds_config["database"] = db

                            dyn_ds_name = f"{ds_name}_{sc_comp}_{table.replace('_', '')}"
                            dynamic_project_config["datasources"][
                                dyn_ds_name
                            ] = table_ds_config
                            log.debug(
                                f"Datasource: {dyn_ds_name} | Config: {table_ds_config}"
                            )
                            source_map_item.append(dyn_ds_name)

                        table_outfile_fqn = get_outfile_fqn(
                            outdir=self.project["outdir"],
                            ds_list=[
                                ds.split(":")[0].replace("_", "")
                                for ds in source_map_item
                            ],
                            infix="comparison",
                        )

                        # Execute CompareTask
                        log.debug(f"Executing CompareTask for: {source_map_item}")
                        CompareTask(
                            profile=self.profile,
                            project=dynamic_project_config,
                            runtime=self.runtime,
                            datasources=source_map_item,
                            outfile_fqn=table_outfile_fqn,
                            sample_count=self.sample_count,
                            composite=self.composite,
                        ).execute()

        end_time = time.time()
        log.info(f"Finished task: scan{' --compare' if self.compare else ''}")
        log.info(f"Total time taken: {(end_time - start_time):.2f} seconds")
