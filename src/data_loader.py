# src/data_loader.py
import sqlite3
import os
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
    df=df.copy()  # Avoid modifying the original DataFrame
    df.drop_duplicates(inplace=True)  # Remove duplicates to prevent integrity errors
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, index=False, if_exists='replace')

def load_walmart_data():
    # Try PostgreSQL first
    try:
        from sqlalchemy import create_engine
        engine_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(engine_url)
        df = pd.read_sql("SELECT * FROM sales;", engine)
        print("Loaded data from PostgreSQL")
        return df
    except Exception as e:
        print("PostgreSQL failed, using SQLite fallback:", e)
        conn = sqlite3.connect('../data/walmart.db')
        df = pd.read_sql("SELECT * FROM sales;", conn)
        conn.close()
        print("Loaded data from SQLite")
        return df
engine_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def load_sql_with_fallback(query, sqlite_path='../data/walmart.db'):
    """
    Execute a SQL query using PostgreSQL if available, else fall back to SQLite.
    
    Parameters:
        query (str): SQL query to execute.
        sqlite_path (str): Path to the SQLite database file.
        
    Returns:
        pd.DataFrame: Query results as a DataFrame.
    """
    try:
        engine_url = (
            f"postgresql://{os.getenv('DB_USER')}:"
            f"{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:"
            f"{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        df = load_postgres(query, engine_url)
        return df
    except Exception as e:
        print("PostgreSQL failed, falling back to SQLite:", e)
        conn = sqlite3.connect(sqlite_path)
        df = pd.read_sql(query, conn)
        conn.close()
        print("Loaded from SQLite")
        return df