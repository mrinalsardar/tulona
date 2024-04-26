import logging
import os
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Union

from tulona.config.runtime import RunConfig
from tulona.exceptions import TulonaMissingPropertyError
from tulona.task.base import BaseTask
from tulona.task.compare import CompareTask
from tulona.task.helper import perform_comparison
from tulona.util.excel import dataframes_into_excel
from tulona.util.filesystem import create_dir_if_not_exist
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
    final_outdir: Union[Path, str]
    compare: bool = DEFAULT_VALUES["compare_scans"]
    sample_count: int = DEFAULT_VALUES["sample_count"]
    composite: bool = DEFAULT_VALUES["compare_column_composite"]

    def execute(self):
        log.info(f"Starting task: scan{' --compare' if self.compare else ''}")
        log.debug(f"Datasource: {self.datasources}")
        log.debug(f"Compare: {self.compare}")
        log.debug(f"Full output directory: {self.final_outdir}")
        start_time = time.time()

        scan_result = {}
        ds_name_compressed_list = []
        connection_profile_names = []
        primary_keys = []
        ds_config_list = []
        for ds_name in self.datasources:
            log.info(f"Processing datasource {ds_name}")
            ds_compressed = ds_name.replace("_", "")
            ds_name_compressed_list.append(ds_compressed)
            ds_config = self.project["datasources"][ds_name]
            ds_config_list.append(ds_config)
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
            scan_result[ds_name]["dbtype"] = dbtype.lower()

            connection_profile = get_connection_profile(self.profile, ds_config)
            conman = self.get_connection_manager(conn_profile=connection_profile)

            # MySQL doesn't have logical database
            if "database" in ds_config:
                database = ds_config["database"]
            elif dbtype.lower() == "mysql":
                database = "def"
            else:
                raise TulonaMissingPropertyError(
                    "Datasource config is missing 'database'."
                )

            # Create output directory
            _ = create_dir_if_not_exist(self.final_outdir)

            # Database scan
            log.debug(f"Performing database scan for: {database}")
            if "schema" in ds_config:
                schemata_query = f"""
                select
                    *
                from
                    {"" if dbtype == "mysql" else database + "."}information_schema.schemata
                where
                    upper(catalog_name) = '{database.upper()}'
                    and upper(schema_name) = '{ds_config["schema"].upper()}'
                """
            else:
                schemata_query = f"""
                select
                    *
                from
                    {"" if dbtype == "mysql" else database + "."}information_schema.schemata
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
                # Writing database scan result
                dbscan_outfile_fqn = Path(
                    self.final_outdir, f"{database.replace('_', '')}_scan.xlsx"
                )
                log.debug(f"Writing db scan result into: {dbscan_outfile_fqn}")
                dataframes_into_excel(
                    sheet_df_map={database: dbextract_df},
                    outfile_fqn=dbscan_outfile_fqn,
                    mode="a" if os.path.exists(dbscan_outfile_fqn) else "w",
                )

            scan_result[ds_name]["database"][database.lower()] = dbextract_df

            # Schema scan
            schema_list = dbextract_df["schema_name"].tolist()
            for schema in schema_list:
                log.debug(f"Performing schema scan for: {database}.{schema}")
                tables_query = f"""
                select
                    *
                from
                    {"" if dbtype == "mysql" else database + "."}information_schema.tables
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
                    # Writing schema scan result
                    schemascan_outfile_fqn = Path(
                        self.final_outdir,
                        f"{database.replace('_', '')}_{schema.replace('_', '')}_scan.xlsx",
                    )
                    log.debug(
                        f"Writing schema scan result into: {schemascan_outfile_fqn}"
                    )
                    dataframes_into_excel(
                        sheet_df_map={schema: schemaextract_df},
                        outfile_fqn=schemascan_outfile_fqn,
                        mode="a" if os.path.exists(schemascan_outfile_fqn) else "w",
                    )
                scan_result[ds_name]["schema"][
                    f"{database.lower()}.{schema.lower()}"
                ] = schemaextract_df

        if self.compare:
            log.debug("Preparing metadata comparison")

            # Handle primary keys for table comparison
            table_primary_key = list(set(primary_keys))
            if not table_primary_key or len(primary_keys) != len(ds_name_compressed_list):
                log.warning(
                    "Primary key is not specified for one/all"
                    f" comparison candidate datasources: {self.datasources}"
                    " Tulona will attempt to extract it from table metadata."
                )
                table_primary_key = None
            if table_primary_key and len(table_primary_key) > 1:
                log.warning(
                    "Primary key[s] must be same for all candidate datasources for comparison"
                    " otherwise table comparison won't work."
                )
                table_primary_key = None

            # Compare database extracts
            databases = [list(scan_result[k]["database"].keys())[0] for k in scan_result]
            db_frames = [
                list(scan_result[k]["database"].values())[0] for k in scan_result
            ]
            dbtypes = [scan_result[k]["dbtype"] for k in scan_result]
            log.debug(f"Comparing databases: {' vs '.join(databases)}")
            # TODO: schema name, if mentioned in the config, can be different
            # for two data sources but still can be comparison candidate
            # or maybe there is no need
            # outer join will keep both rows
            db_comp = perform_comparison(
                ds_compressed_names=databases,
                dataframes=db_frames,
                on="schema_name",
                how="outer",
                suffixes=ds_name_compressed_list,
                indicator="presence",
            )
            db_comp["presence"] = db_comp["presence"].map(
                {
                    "both": "both",
                    "left_only": ds_name_compressed_list[0],
                    "right_only": ds_name_compressed_list[1],
                }
            )

            # Writing database comparison result
            dbs_compressed = [db.replace("_", "") for db in databases]
            dbcomp_outfile_fqn = Path(
                self.final_outdir, f"{'_'.join(dbs_compressed)}_comparison.xlsx"
            )
            log.debug(f"Writing db scan comparison result into: {dbcomp_outfile_fqn}")
            dataframes_into_excel(
                sheet_df_map={f"db_{'|'.join(databases)}": db_comp},
                outfile_fqn=dbcomp_outfile_fqn,
                mode="a" if os.path.exists(dbcomp_outfile_fqn) else "w",
            )

            # Compare schema extracts: list[list[Dict, Dict]]
            # [
            #     [
            #         {"datasource": "ds1", "database": "db1", "schema": "sc1"},
            #         {"datasource": "ds2", "database": "db2", "schema": "sc2"},
            #     ],
            # ]
            schema_combinations = []
            schemas_from_config = [c["schema"] for c in ds_config_list if "schema" in c]
            if len(schemas_from_config) == 0:
                common_schemas = db_comp[db_comp["presence"] == "both"][
                    "schema_name"
                ].tolist()
                for sc in common_schemas:
                    schema_combinations.append(
                        [
                            {"datasource": ds, "database": db, "schema": sc}
                            for ds, db, sc in zip(
                                ds_name_compressed_list,
                                databases,
                                [sc] * len(ds_name_compressed_list),
                            )
                        ]
                    )
            elif len(schemas_from_config) == len(ds_config_list):
                schema_combinations = [
                    [
                        {"datasource": ds, "database": db, "schema": sc}
                        for ds, db, sc in zip(
                            ds_name_compressed_list, databases, schemas_from_config
                        )
                    ]
                ]
            else:
                log.warning(
                    "Cannot perform schema comparison if schema is specified for some candidate"
                    " datasources but not all. If particular schemas need to be"
                    " scanned and compared, schema must be specified for both datasources."
                    " If schema is not specified for any of them, scan and compare"
                    " will be performed at database level, for all the schemas."
                )

            log.debug(
                f"Number of schema combinations to compare: {len(schema_combinations)}"
            )

            for scombo in schema_combinations:
                log.debug(f"Comparing schema: {scombo}")
                schema_fqns = [f"{cand['database']}.{cand['schema']}" for cand in scombo]
                schema_compressed = [
                    sf.replace(".", "").replace("_", "") for sf in schema_fqns
                ]
                schema_frames = [
                    scan_result[ds_name]["schema"][sf.lower()]
                    for ds_name, sf in zip(self.datasources, schema_fqns)
                ]

                schema_comp = perform_comparison(
                    schema_fqns,
                    schema_frames,
                    on="table_name",
                    how="outer",
                    suffixes=schema_compressed,
                    indicator="presence",
                )
                schema_comp["presence"] = schema_comp["presence"].map(
                    {
                        "both": "both",
                        "left_only": ds_name_compressed_list[0],
                        "right_only": ds_name_compressed_list[1],
                    }
                )

                # Writing schema comparison result
                schemacomp_outfile_fqn = Path(
                    self.final_outdir, f"{'_'.join(schema_compressed)}_comparison.xlsx"
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
                common_tables = schema_comp[schema_comp["presence"] == "both"][
                    "table_name"
                ].tolist()
                log.debug(f"Number of common_tables found: {len(common_tables)}")

                dynamic_project_config = deepcopy(self.project)
                dynamic_project_config["datasources"] = {}
                if "source_map" in dynamic_project_config:
                    dynamic_project_config.pop("source_map")
                for table in common_tables:
                    log.debug(f"Comparing table: {scombo} - {table}")

                    source_map_item = []
                    for cand, typ, cpn in zip(scombo, dbtypes, connection_profile_names):
                        table_ds_config = {
                            "connection_profile": cpn,
                            "schema": cand["schema"],
                            "table": table,
                        }
                        if table_primary_key:
                            table_ds_config["primary_key"] = table_primary_key
                            table_ds_config["compare_column"] = table_primary_key
                        if typ != "mysql":
                            table_ds_config["database"] = cand["database"]

                        sc_comp = cand["schema"].replace("_", "")
                        dyn_ds_name = f"{ds_name}_{sc_comp}_{table.replace('_', '')}"
                        dynamic_project_config["datasources"][
                            dyn_ds_name
                        ] = table_ds_config
                        log.debug(
                            f"Datasource: {dyn_ds_name} | Config: {table_ds_config}"
                        )
                        source_map_item.append(dyn_ds_name)

                    table_fqns = [f"{sf}_{table}" for sf in schema_compressed]
                    table_outfile_fqn = Path(
                        self.final_outdir,
                        f"{'_'.join(table_fqns)}_comparison.xlsx",
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

        exec_time = time.time() - start_time
        compare_flag = " --compare" if self.compare else ""
        log.info(f"Finished task: scan{compare_flag} in {exec_time:.2f} seconds")
