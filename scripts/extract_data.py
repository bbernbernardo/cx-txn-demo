import pandas as pd
import psycopg2
import os
from psycopg2.extras import execute_batch

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

CSV_FILE = os.path.join("..", "data", "customer_transactions.csv")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "cx_txn",
    "user": "db_user",
    "password": "db_password"
}

TARGET_TABLE = "demo.customer_transactions"

# ------------------------------------------------------------------------------
# Step 1: Read CSV and ensure all columns are strings
# ------------------------------------------------------------------------------

# Read CSV file
try:
    df = pd.read_csv(CSV_FILE, dtype=str)
except Exception as e:
    print(f"Unexpected error reading '{CSV_FILE}': {e}")
    raise

# Strip whitespace and fill NaN with None
df = df.applymap(lambda x: str(x).strip() if pd.notnull(x) else None)

print(f"Loaded {len(df)} rows from {CSV_FILE}")

# ------------------------------------------------------------------------------
# Step 2: Create target table if it doesn’t exist (all columns VARCHAR)
# ------------------------------------------------------------------------------

create_table_sql = f"""
CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    transaction_id VARCHAR,
    customer_id VARCHAR,
    transaction_date VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    quantity VARCHAR,
    price VARCHAR,
    tax VARCHAR
);
"""

# Prepare insert statement
insert_sql = f"""
INSERT INTO {TARGET_TABLE} (
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    quantity,
    price,
    tax
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Convert DataFrame to list of tuples
records = [tuple(row) for row in df.to_numpy()]
# ------------------------------------------------------------------------------
# Step 3: Insert data into Postgres
# ------------------------------------------------------------------------------

try:
    # Connect to Database
    print("Connection to Database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Connection successful")

    # Create table
    print("Creating schema and table if not exists")
    cur.execute(create_table_sql)
    conn.commit()
    print("Table creation successful")

    # Use execute_batch for performance to insert records
    print("Inserting records")
    execute_batch(cur, insert_sql, records, page_size=5000)
    conn.commit()
    print(f"Successfully inserted {len(records)} rows into {TARGET_TABLE}")

except psycopg2.Error as e:
    print(f"PSQL execution error: {e}")
    raise

finally:
    cur.close()
    conn.close()
