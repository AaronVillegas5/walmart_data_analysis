import sqlite3
import pandas as pd
from sqlalchemy import create_engine

def load_sqlite(db_path='../data/walmart.db', table_name='sales'):
    """Load data from SQLite DB."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def load_postgres(query, engine_url):
    """Load data from PostgreSQL."""
    engine = create_engine(engine_url)
    df = pd.read_sql(query, engine)
    return df

def load_csv(csv_path='../data/Walmart.csv'):
    """Load data directly from CSV."""
    return pd.read_csv(csv_path)