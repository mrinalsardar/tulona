import logging
import random
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, schema

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


conn_str_list = [
    'postgresql://postgres:postgres@db_pg:5432/postgres',
    'mysql+pymysql://user:password@db_mysql:3306/db'
]

if __name__ == '__main__':
    for csvf in Path(Path().absolute(), 'tests', 'fixtures', 'test_data').glob('*.csv'):
        log.info("Loading sample data into postgres and mysql tables")

        schema_name = 'medical'
        table_name = csvf.name.split('.')[0]
        log.debug(f"Loading to table: {table_name}")

        for cstr in conn_str_list:
            engine = create_engine(cstr)

            log.debug(f"Reading file: {csvf}")
            df = pd.read_csv(csvf)

            keep = int((df.shape[0] / 100) * random.randint(80, 100))
            df = df.sample(keep).sort_index()

            log.debug(f"Loading {df.shape[0]} records into {cstr.split(':')[0]}.{table_name}")
            with engine.connect() as conn:
                # conn.execute(f"create schema if not exists {schema_name}")
                # conn.execute(schema.CreateSchema(name=f"{cstr.split('/')[-1]}.{schema_name}", if_not_exists=True))
                # if not conn.dialect.has_schema(engine, schema_name):
                #     engine.execute(schema.CreateSchema(schema_name))

                # df.to_sql(table_name, conn, schema='medical', if_exists='replace')
                df.to_sql(table_name, conn, if_exists='replace')
