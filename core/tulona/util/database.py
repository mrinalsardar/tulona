from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.orm import defer, mapper


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


def get_table_reflection(engine, schema, name):
    reflection = Table(name, MetaData(), schema=schema, autoload_with=engine)
    return reflection


def get_table_model(table_reflection):
    class GenericTable:
        pass

    return mapper(GenericTable, table_reflection)


def build_query(conman, table_reflection: Table, exclude_columns: list[str]):
    GenericTable = get_table_model(table_reflection)
    query = conman.session.query(GenericTable)
    query = query.options(*[defer(c) for c in exclude_columns])
    return query
