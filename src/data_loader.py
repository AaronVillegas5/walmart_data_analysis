# src/data_loader.py
import sqlite3
import pandas as pd
from sqlalchemy import create_engine

def load_csv(csv_path='../data/Walmart.csv'):
    """Load data from CSV"""
    return pd.read_csv(csv_path)

def load_sqlite(db_path='../data/walmart.db', table_name='sales'):
    """Load data from SQLite"""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def load_postgres(query, engine_url):
    """Load data from PostgreSQL"""
    engine = create_engine(engine_url)
    df = pd.read_sql(query, engine)
    return df
def save_to_sqlite(df, db_path='../data/walmart.db', table_name='sales'):
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, index=False, if_exists='replace')