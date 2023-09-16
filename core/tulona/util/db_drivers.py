
def get_db_driver(dbtype: str) -> str:
    return {
        'postgres': 'postgresql',
        'mysql': 'mysql+pymysql',
    }[dbtype]