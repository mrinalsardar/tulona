import logging
import random
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("faker").setLevel(logging.ERROR)


conn_str_list = [
    "postgresql://postgres:postgres@localhost:5432/postgres",
    "mysql+pymysql://user:password@localhost:3306/db",
]

if __name__ == "__main__":
    log.info("Loading sample data into postgres and mysql tables")
    for csvf in Path(Path(__file__).resolve().parent, "data").glob("*.csv"):
        log.debug(f"Loding file: {csvf}")
        # schema_name = 'corporate'
        table_name = csvf.name.split(".")[0]
        log.debug(f"Loading to table: {table_name}")

        for cstr in conn_str_list:
            engine = create_engine(cstr)

            log.debug(f"Reading file: {csvf}")
            df = pd.read_csv(csvf)

            # Changing some values randomly
            rows_to_modify = df.sample(frac=0.5).index
            df.loc[rows_to_modify, "Age"] = np.random.randint(
                10, 60, size=len(rows_to_modify)
            )

            rows_to_modify = df.sample(frac=0.5).index
            new_values = np.random.choice(
                list({Faker().job() for _ in range(5)}), size=len(rows_to_modify)
            )
            df.loc[rows_to_modify, "Department"] = new_values

            # Converting date time columns from string to datetime
            df['Employment_Date'] = pd.to_datetime(df['Employment_Date'])
            df['Date_of_Birth'] = pd.to_datetime(df['Date_of_Birth'])

            # Dropping random rows
            keep = int((df.shape[0] / 100) * random.randint(80, 100))
            df = df.sample(keep).sort_index()

            log.debug(
                f"Loading {df.shape[0]} records into {cstr.split(':')[0]}.{table_name}"
            )
            with engine.connect() as conn:
                # conn.execute(f"create schema if not exists {schema_name}")
                # conn.execute(
                #     schema.CreateSchema(
                #         name=f"{cstr.split('/')[-1]}.{schema_name}",
                #         if_not_exists=True
                #     )
                # )
                # if not conn.dialect.has_schema(engine, schema_name):
                #     engine.execute(schema.CreateSchema(schema_name))

                # df.to_sql(table_name, conn, schema=schema_name, if_exists='replace', index=False)
                df.to_sql(table_name, conn, if_exists="replace", index=False)
