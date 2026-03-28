import pandas as pd
import os, sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

file_path = os.path.join(os.path.dirname(__file__), "..", "data/Walmart.csv")
df = pd.read_csv(file_path)

# Clean column names and data types
df.columns = df.columns.str.strip().str.lower()
df['holiday_flag'] = df['holiday_flag'].astype(bool)
df['date'] = pd.to_datetime(df['date'], dayfirst=True)

# Connect to PostgreSQL
engine_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(engine_url)
print(os.getcwd())


# Insert into PostgreSQL
df.to_sql('sales', engine, index=False, if_exists='append')

# Verify data
query = "SELECT * FROM sales LIMIT 1;"
df_test = pd.read_sql(query, engine)
print("PostgreSQL preview:")
print(df_test)