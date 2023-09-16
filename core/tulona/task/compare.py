import logging
import dask.dataframe as ddf
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
from tulona.task.base import BaseTask
from tulona.adapter.connection import ConnectionManager
from tulona.util.database import (
    get_schemas_from_db,
    get_table_primary_keys,
    get_tables_from_schema,
    get_tables_from_db
)
from tulona.exceptions import (
    TulonaUnSupportedExecEngine
)
from tulona.util.filesystem import get_result_dir
from typing import Dict


log = logging.getLogger(__name__)


RESTULT_LOCATIONS = {
    "result_dir": "results",
    "metadiff_dir": "metadata",
    "result_meta_outfile": "result_metadata.csv",
    "datadiff_dir": "datadiff",
}

DEFAULT_DATABASE = {
    "postgres": 'public',
    "mysql": 'mydb',
}


@dataclass
class CompareTask(BaseTask):
    eligible_conn_profiles: list
    runtime: Dict[str, str]

    def get_connections(self, profiles: list[Dict]) -> list:
        connections = []
        for prof in profiles:
            conman = ConnectionManager(
                host=prof['host'],
                username=prof['username'],
                port=prof['port'],
                password=prof['password'],
                database=prof['database'],
                dbtype=prof['type']
            )
            conman.open()

            connections.append(conman)

        return connections


    def prepare_rows_pandas(self, df: pd.DataFrame, tulona_id_col: str) -> pd.DataFrame:
        # return pd.DataFrame(pd.util.hash_pandas_object(obj=df, index=False))

        # TODO: handle timestamp columns - convert them to a standard format
        # df[tulona_id_col] = pd.Series(df.fillna('').values.tolist()).str.join('|||')
        df = pd.DataFrame(
            data=pd.Series(
                df.fillna('').values.tolist()).map(lambda x: '|||'.join(map(str,x))
            ),
            columns=['concatenated_record']
        )
        df[tulona_id_col] = pd.util.hash_pandas_object(obj=df, index=False)
        return df


    def compare_tables_pandas(self, connections, schema1, schema2, tab1, tab2=None, primary_key=[]):
        tab2 = tab2 if tab2 else tab1
        tulona_id_col = "tulona_row_id"

        df1 = pd.read_sql_table(table_name=tab1, con=connections[0].conn)
        df2 = pd.read_sql_table(table_name=tab2, con=connections[1].conn)

        # ---------> Level 1: match the hashes of whole row
        df1 = self.prepare_rows_pandas(df1, tulona_id_col=tulona_id_col)
        df2 = self.prepare_rows_pandas(df2, tulona_id_col=tulona_id_col)

        df_merge = pd.merge(
            left=df1,
            right=df2,
            on=tulona_id_col,
            how="outer",
            suffixes=(f'_{connections[0].database}', f'_{connections[1].database}'),
            indicator=True
        )

        # mismtaches
        df_mismatch = df_merge[df_merge['_merge'] != 'both'].drop(tulona_id_col, axis=1)

        # ---------> Level 2+: TODO


        # ---------> Result
        # metadiff
        df1_extra_cols = list(set(df1.columns).difference(set(df2.columns)))
        df2_extra_cols = list(set(df2.columns).difference(set(df1.columns)))

        self.result_meta.append(
            [
                connections[0].database,
                connections[1].database,
                schema1,
                schema2,
                tab1,
                tab2,
                df1.shape[0],
                df2.shape[0],
                df_merge[df_merge['_merge'] == 'both'].shape[0],
                df_merge[df_merge['_merge'] == 'left_only'].shape[0],
                df_merge[df_merge['_merge'] == 'right_only'].shape[0],
                ', '.join(df1_extra_cols),
                ', '.join(df2_extra_cols),
            ]
        )

        # datadiff
        if df_mismatch.shape[0] > 0:
            diffpath = Path(
                self.results_dir,
                RESTULT_LOCATIONS['datadiff_dir'],
                f"{connections[0].database}__{connections[1].database}",
                f"{schema1}__{schema2}"
            )
            diffpath.mkdir(parents=True, exist_ok=True)
            diffcsv = Path(diffpath, f"{tab1}__{tab2}.csv")

            log.debug(f"Writing {tab1} vs {tab2} diff into: {diffcsv}")
            df_mismatch.to_csv(diffcsv, header=True, index=False)


    def compare_tables_dask(self, connections, tab1, tab2):
        df1 = ddf.read_sql_table(table_name=tab1, con=connections[0].conn)
        df2 = ddf.read_sql_table(table_name=tab2, con=connections[1].conn)

        # TODO


    def compare_tables(self, connections, schema1: str, schema2: str, tab1: str, tab2: str=None):
        tab2 = tab2 if tab2 else tab1

        tab1_pk = get_table_primary_keys(engine=connections[0].engine, table=tab1)
        tab2_pk = get_table_primary_keys(engine=connections[1].engine, table=tab2)

        primary_key_available = len(tab1_pk) > 0 and len(tab2_pk) > 0 and tab1_pk == tab2_pk

        if primary_key_available:
            log.debug(f"Primary key available for table: {tab1}")
        else:
            log.debug(f"Primary key unavailable for table: {tab1}")

        if self.runtime.engine.lower() == 'pandas':
            self.compare_tables_pandas(
                connections=connections,
                schema1=schema1,
                schema2=schema2,
                primary_key=tab1_pk if primary_key_available else [],
                tab1=tab1,
                tab2=tab2
            )
        elif self.runtime.engine.lower() == 'dask':
            self.compare_tables_dask(
                connections=connections,
                schema1=schema1,
                schema2=schema2,
                primary_key=tab1_pk if primary_key_available else [],
                tab1=tab1,
                tab2=tab2
            )
        else:
            raise TulonaUnSupportedExecEngine(
                f"Execution engine {self.runtime.engine} is not supported yet!"
            )


    def compare_schemas(self, connections, schema1: str, schema2: str=None):
        schema2 = schema2 if schema2 else schema1

        schema1_tables = get_tables_from_schema(engine=connections[0].engine, schema=schema1)
        schema2_tales = get_tables_from_schema(engine=connections[1].engine, schema=schema2)

        eligible_tables = list(set(schema1_tables).intersection(set(schema2_tales)))
        log.debug(f"Tables eligible for comparison: {eligible_tables}")

        for tab in eligible_tables:
            log.debug(f"Compare table: {tab}")
            self.compare_tables(connections=connections, schema1=schema1, schema2=schema2, tab1=tab)


    def compare_databases(self, connections, ignore_schema):
        if ignore_schema:
            db1_tables = get_tables_from_db(engine=connections[0].engine)
            db2_tables = get_tables_from_db(engine=connections[1].engine)

            eligible_tables = list(set(db1_tables).intersection(set(db2_tables)))
            # db1_extra_tables = list(set(db1_tables).difference(set(db2_tables)))
            # db2_extra_tables = list(set(db2_tables).difference(set(db1_tables)))

            for tab in eligible_tables:
                log.debug(f"Compare table: {tab}")
                self.compare_tables(
                    connections=connections,
                    schema1=DEFAULT_DATABASE[connections[0].dbtype],
                    schema2=DEFAULT_DATABASE[connections[1].dbtype],
                    tab1=tab,
                    tab2=tab
                )
        else:
            db1_schemas = get_schemas_from_db(engine=connections[0].engine)
            db2_schemas = get_schemas_from_db(engine=connections[1].engine)

            eligible_schemas = list(
                set(db1_schemas)
                .intersection(set(db2_schemas))
                .difference({"information_schema"})
            )
            log.debug(f"Schemas eligible for comparison: {eligible_schemas}")

            for sch in eligible_schemas:
                log.debug(f"Compare schema: {sch}")
                self.compare_schemas(connections=connections, schema1=sch)


    def execute(self):
        log.info("Starting comparison")
        log.debug("******************** Task profile ********************")
        log.debug(f"******* * Execution engine: {self.runtime.engine}")
        log.debug(f"******* * Ignore schema: {self.runtime.ignore_schema}")
        log.debug(f"*****************************************************")
        log.debug("")

        self.result_meta = []
        self.results_dir = get_result_dir(
            dir_dict=RESTULT_LOCATIONS,
            base=self.runtime.outdir,
            key='result_dir'
        )

        for profile_combo in self.eligible_conn_profiles:
            log.debug("Creating connection instances")
            connections = self.get_connections(profiles=profile_combo)

            profile_info = [f"{d['type']}:{d['host']}:{d['database']}" for d in profile_combo]
            log.debug(f"Comparing databases: {profile_info}")
            self.compare_databases(connections=connections, ignore_schema=self.runtime.ignore_schema)

            log.debug(f"Closing connections")
            for c in connections:
                c.close()

        # metadata comparison
        result_meta_cols = [
            'db1_name',
            'db2_name',
            'schema1_name',
            'schema2_name',
            'db1_table_name',
            'db2_table_name',
            'db1_rowcount',
            'db2_rowcount',
            'matched_rowcount',
            'db1_extra_row_count',
            'db2_extra_row_count',
            'db1_extra_cols',
            'db2_extra_cols',
        ]
        metadiff_dir = Path(
            self.results_dir,
            RESTULT_LOCATIONS['metadiff_dir']
        )
        metadiff_dir.mkdir(parents=True, exist_ok=True)
        metadiff_file = Path(metadiff_dir, RESTULT_LOCATIONS['result_meta_outfile'])
        log.debug(f"Writing table comparison metadata into: {metadiff_file}")
        df_result_meta = pd.DataFrame(data=self.result_meta, columns=result_meta_cols)
        df_result_meta.to_csv(metadiff_file, header=True, index=False)
