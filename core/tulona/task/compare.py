import logging
import dask.dataframe as dd
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
from tulona.task.base import BaseTask
from tulona.adapter.connection import ConnectionManager
from tulona.util.database import (
    get_schemas_from_db,
    get_table_primary_keys,
    get_tables_from_schema,
)
from tulona.exceptions import TulonaUnSupportedExecEngine
from tulona.util.filesystem import get_result_dir
from tulona.config.runtime import RunConfig
from typing import Dict, Union, Tuple


log = logging.getLogger(__name__)


RESTULT_LOCATIONS = {
    "result_dir": "results",
    "metadiff_dir": "metadata",
    "result_meta_outfile": "result_metadata.csv",
    "datadiff_dir": "datadiff",
}

DEFAULT_DATABASE = {
    "postgres": "public",
    "mysql": "mydb",
}

RESULT_META_COLS = [
    "db1_name",
    "db2_name",
    "schema1_name",
    "schema2_name",
    "db1_table_name",
    "db2_table_name",
    "compared_on",
    "db1_rowcount",
    "db2_rowcount",
    "matched_rowcount",
    "db1_extra_row_count",
    "db2_extra_row_count",
    "db1_extra_cols",
    "db2_extra_cols",
]


@dataclass
class CompareTask(BaseTask):
    profile: Dict
    project: Dict
    runtime: RunConfig

    def get_connection(
        self,
        dbtype: str,
        host: str,
        port: Union[str, int],
        username: str,
        password: str,
        database: str,
    ) -> ConnectionManager:
        conman = ConnectionManager(
            dbtype=dbtype,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        conman.open()

        return conman

    def prepare_rows_pandas(self, df: pd.DataFrame, row_hash_col: str) -> pd.DataFrame:
        # return pd.DataFrame(pd.util.hash_pandas_object(obj=df, index=False))

        # TODO: handle timestamp columns - convert them to a standard format
        # df[row_hash_col] = pd.Series(df.fillna('').values.tolist()).str.join('|||')
        df = pd.DataFrame(
            data=pd.Series(df.fillna("").values.tolist()).map(
                lambda x: "|||".join(map(str, x))
            ),
            columns=["concat_value"],
        )
        df[row_hash_col] = pd.util.hash_pandas_object(obj=df, index=False)
        return df

    def compare_tables_pandas(
        self,
        connection1: ConnectionManager,
        connection2: ConnectionManager,
        schema1: str,
        schema2: str,
        tab1: str,
        tab2: str,
        primary_key: Union[str, list] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        row_hash_col = "row_hash"
        primary_key = list(primary_key) if primary_key else None

        df1 = pd.read_sql_table(table_name=tab1, con=connection1.conn)
        df2 = pd.read_sql_table(table_name=tab2, con=connection2.conn)

        # ---------> Level 1: match the hashes of whole row
        if primary_key:  # TODO: Need to test this
            df1 = pd.concat(
                [
                    df1[primary_key],
                    self.prepare_rows_pandas(df1, row_hash_col=row_hash_col),
                ],
                axis=1,
            )
            df2 = pd.concat(
                [
                    df2[primary_key],
                    self.prepare_rows_pandas(df2, row_hash_col=row_hash_col),
                ],
                axis=1,
            )
        else:
            df1 = self.prepare_rows_pandas(df1, row_hash_col=row_hash_col)
            df2 = self.prepare_rows_pandas(df2, row_hash_col=row_hash_col)

        df_merge = pd.merge(
            left=df1,
            right=df2,
            on=primary_key if primary_key else row_hash_col,
            how="outer",
            suffixes=(f"_{connection1.database}", f"_{connection2.database}"),
            indicator=True,
        )

        # mismtaches
        df_mismatch = df_merge[df_merge["_merge"] != "both"].drop(row_hash_col, axis=1)

        # ---------> Level 2+: TODO

        # ---------> Result
        # metadiff
        df1_extra_cols = list(set(df1.columns).difference(set(df2.columns)))
        df2_extra_cols = list(set(df2.columns).difference(set(df1.columns)))

        metadata = [
            connection1.database,
            connection2.database,
            schema1,
            schema2,
            tab1,
            tab2,
            primary_key if primary_key else row_hash_col,
            df1.shape[0],
            df2.shape[0],
            df_merge[df_merge["_merge"] == "both"].shape[0],
            df_merge[df_merge["_merge"] == "left_only"].shape[0],
            df_merge[df_merge["_merge"] == "right_only"].shape[0],
            ", ".join(df1_extra_cols),
            ", ".join(df2_extra_cols),
        ]
        df_meta = pd.DataFrame(data=[metadata], columns=RESULT_META_COLS)

        return df_mismatch, df_meta

    def compare_tables_dask(
        self,
        connection1: ConnectionManager,
        connection2: ConnectionManager,
        schema1: str,
        schema2: str,
        tab1: str,
        tab2: str,
        primary_key: list = [],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # TODO: Implement

        return pd.DataFrame(), pd.DataFrame()

    def compare_tables(
        self,
        connection1: ConnectionManager,
        connection2: ConnectionManager,
        schema1: str,
        schema2: str,
        tab1: str,
        tab2: str,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        tab2 = tab2 if tab2 else tab1

        tab1_pk = get_table_primary_keys(engine=connection1.engine, table=tab1)
        tab2_pk = get_table_primary_keys(engine=connection2.engine, table=tab2)

        primary_key_available = (
            len(tab1_pk) > 0 and len(tab2_pk) > 0 and tab1_pk == tab2_pk
        )

        if primary_key_available:
            log.debug(f"Primary key available for table: {tab1}")
        else:
            log.debug(f"Primary key unavailable for table: {tab1}")

        if self.runtime.engine.lower() == "pandas":
            df_mismatch, df_meta = self.compare_tables_pandas(
                connection1=connection1,
                connection2=connection2,
                schema1=schema1,
                schema2=schema2,
                primary_key=tab1_pk if primary_key_available else [],
                tab1=tab1,
                tab2=tab2,
            )
        elif self.runtime.engine.lower() == "dask":
            df_mismatch, df_meta = self.compare_tables_dask(
                connection1=connection1,
                connection2=connection2,
                schema1=schema1,
                schema2=schema2,
                primary_key=tab1_pk if primary_key_available else [],
                tab1=tab1,
                tab2=tab2,
            )
        else:
            raise TulonaUnSupportedExecEngine(
                f"Execution engine {self.runtime.engine} is not supported yet!"
            )

        return df_mismatch, df_meta

    def prepare_table_list(
            self,
            config: list[Dict],
            level: str,
            table_combo_list: list=[]
        ) -> list[Dict]:
        # TODO: consider using generator for table_combo_list as this list can be long
        all_profiles = self.profile["profiles"]

        log.debug(f"Preparing for {level=}")
        if level.lower() == "database":
            for combo in config:
                log.debug(f"Preparing {combo=}")
                profile1 = all_profiles[combo["profile1"]]
                profile2 = all_profiles[combo["profile2"]]

                conn1 = self.get_connection(
                    dbtype=profile1["type"],
                    host=profile1["host"],
                    port=profile1["port"],
                    username=profile1["username"],
                    password=profile1["password"],
                    database=combo["database1"],
                )
                conn2 = self.get_connection(
                    dbtype=profile2["type"],
                    host=profile2["host"],
                    port=profile2["port"],
                    username=profile2["username"],
                    password=profile2["password"],
                    database=combo["database2"],
                )

                database1_schemas = get_schemas_from_db(engine=conn1.engine)
                database2_schemas = get_schemas_from_db(engine=conn2.engine)

                conn1.close()
                conn2.close()

                # To compare schemas from two databases, schema names should be same in both
                # Also no need to process information_schema of course
                common_schemas = list(
                    set(database1_schemas)
                    .intersection(set(database2_schemas))
                    .difference({"information_schema"})
                )
                database1_extra_schemas = list(
                    set(database1_schemas).difference(set(database2_schemas))
                )
                database2_extra_schemas = list(
                    set(database2_schemas).difference(set(database1_schemas))
                )

                log.debug(f"{common_schemas=}")

                schema_level_config = [
                    {
                        "database1": combo["database1"],
                        "schema1": s,
                        "profile1": combo["profile1"],
                        "database2": combo["database2"],
                        "schema2": s,
                        "profile2": combo["profile2"],
                    }
                    for s in common_schemas
                ]

                self.prepare_table_list(
                    config=schema_level_config,
                    level="schema",
                    table_combo_list=table_combo_list,
                )

        elif level.lower() == "schema":
            for combo in config:
                log.debug(f"Preparing {combo=}")
                profile1 = all_profiles[combo["profile1"]]
                profile2 = all_profiles[combo["profile2"]]

                conn1 = self.get_connection(
                    dbtype=profile1["type"],
                    host=profile1["host"],
                    port=profile1["port"],
                    username=profile1["username"],
                    password=profile1["password"],
                    database=combo["database1"],
                )
                conn2 = self.get_connection(
                    dbtype=profile2["type"],
                    host=profile2["host"],
                    port=profile2["port"],
                    username=profile2["username"],
                    password=profile2["password"],
                    database=combo["database2"],
                )

                schema1_tables = get_tables_from_schema(
                    engine=conn1.engine,
                    schema=combo["schema1"]
                )
                schema2_tables = get_tables_from_schema(
                    engine=conn2.engine,
                    schema=combo["schema2"]
                )

                conn1.close()
                conn2.close()

                # To compare tables from to schema, table names should be same in both
                common_tables = list(set(schema1_tables).intersection(set(schema2_tables)))
                schema1_extra_tables = list(set(schema1_tables).difference(set(schema2_tables)))
                schema2_extra_tables = list(set(schema2_tables).difference(set(schema1_tables)))

                table_level_config = [
                    {
                        "database1": combo["database1"],
                        "schema1": combo["schema1"],
                        "table1": t,
                        "profile1": combo["profile1"],
                        "database2": combo["database2"],
                        "schema2": combo["schema2"],
                        "table2": t,
                        "profile2": combo["profile2"],
                    }
                    for t in common_tables
                ]

                self.prepare_table_list(
                    config=table_level_config,
                    level="table",
                    table_combo_list=table_combo_list,
                )

        elif level.lower() == "table":
            for combo in config:
                log.debug(f"Preparing {combo=}")
                profile1 = all_profiles[combo["profile1"]]
                profile2 = all_profiles[combo["profile2"]]

                table_combo_list.append(
                    {
                        "database1": combo["database1"],
                        "schema1": combo["schema1"],
                        "table1": combo["table1"],
                        "profile1": {  # TODO: consider serialization
                            "dbtype": profile1["type"],
                            "host": profile1["host"],
                            "port": profile1["port"],
                            "username": profile1["username"],
                            "password": profile1["password"],
                            "database": combo["database1"],
                        },
                        "database2": combo["database2"],
                        "schema2": combo["schema2"],
                        "table2": combo["table2"],
                        "profile2": {  # TODO: consider serialization
                            "dbtype": profile2["type"],
                            "host": profile2["host"],
                            "port": profile2["port"],
                            "username": profile2["username"],
                            "password": profile2["password"],
                            "database": combo["database2"],
                        },
                    }
                )

        else:
            raise TulonaUnSupportedExecEngine(f"Level {level} not supported")

        return table_combo_list

    def execute(self):
        log.info("Starting comparison")
        log.debug("******************** Task profile ********************")
        log.debug(f"******* * Comparison level: {self.runtime.level}")
        log.debug(f"******* * Execution engine: {self.runtime.engine}")
        log.debug(f"******* * Output directory: {self.runtime.outdir}")
        log.debug(f"*****************************************************")
        log.debug("")

        self.results_dir = get_result_dir(
            dir_dict=RESTULT_LOCATIONS,
            base=self.runtime.outdir,
            key='result_dir'
        )

        table_combo_list = self.prepare_table_list(
            config=self.project[f"{self.runtime.level}s"],
            level=self.runtime.level
        )

        if len(table_combo_list) > 0:
            meta_df_list = []
            for table_combo in table_combo_list:
                log.debug(
                    "Comparing: %s vs %s",
                    "profile1-> " + ".".join([table_combo["database1"], table_combo["schema1"], table_combo["table1"]]),
                    "profile2-> " + ".".join([table_combo["database2"], table_combo["schema2"], table_combo["table2"]])
                )
                conn1 = self.get_connection(**table_combo["profile1"])
                conn2 = self.get_connection(**table_combo["profile2"])

                df_mismatch, df_meta = self.compare_tables(
                    connection1=conn1,
                    connection2=conn2,
                    schema1=table_combo["schema1"],
                    schema2=table_combo["schema2"],
                    tab1=table_combo["table1"],
                    tab2=table_combo["table2"],
                )

                meta_df_list.append(df_meta)

                if df_mismatch.shape[0] > 0:
                    diffpath = Path(
                        self.results_dir,
                        RESTULT_LOCATIONS["datadiff_dir"],
                        f"{conn1.database}__{conn2.database}",
                        f"{table_combo['schema1']}__{table_combo['schema2']}",
                    )
                    diffpath.mkdir(parents=True, exist_ok=True)
                    diffcsv = Path(
                        diffpath,
                        f"{table_combo['table1']}__{table_combo['table2']}.csv",
                    )

                    log.debug(
                        f"Writing {table_combo['table1']} vs {table_combo['table2']} diff"
                        + f" into: {diffcsv}"
                    )
                    df_mismatch.to_csv(diffcsv, header=True, index=False)

                conn1.close()
                conn2.close()

            # metadata comparison
            metadiff_dir = Path(self.results_dir, RESTULT_LOCATIONS["metadiff_dir"])
            metadiff_dir.mkdir(parents=True, exist_ok=True)
            metadiff_file = Path(metadiff_dir, RESTULT_LOCATIONS["result_meta_outfile"])
            log.debug(f"Writing table comparison metadata into: {metadiff_file}")
            df_result_meta = pd.concat(meta_df_list)
            df_result_meta.to_csv(metadiff_file, header=True, index=False)
        else:
            log.warn("Nothing to compare")

        log.info(
            f"Comparison complete. Please look at the '{self.runtime.outdir}' folder for results"
        )
