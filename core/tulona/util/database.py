from sqlalchemy import MetaData, Table, inspect


def get_schemas_from_db(engine):
    inspector = inspect(engine)
    return sorted(inspector.get_schema_names())


def get_tables_from_db(engine):
    inspector = inspect(engine)
    return sorted(inspector.get_table_names())


def get_tables_from_schema(engine, schema):
    inspector = inspect(engine)
    return sorted(inspector.get_table_names(schema=schema))


def get_table_primary_keys(engine, schema, table):
    tabmeta = Table(table, MetaData(), schema=schema, autoload_with=engine)
    return [c.name for c in tabmeta.primary_key.columns.values()]
