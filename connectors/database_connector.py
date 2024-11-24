import pandas as pd
from sqlalchemy import create_engine

def fetch_data_from_database(connection_string, query):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result
