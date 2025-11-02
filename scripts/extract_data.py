import sys
import pandas as pd
import psycopg2
import os
from psycopg2.extras import execute_batch

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
sys.path.append('/import/csv')
CSV_FILE = os.path.join(".", "customer_transactions.csv")
print(f"CSV FILE PATH: {CSV_FILE}")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "cx_txn",
    "user": "db_user",
    "password": "db_password"
}

TARGET_TABLE = "demo.raw_customer_transactions"

# ------------------------------------------------------------------------------
# Step 1: Read CSV and ensure all columns are strings
# ------------------------------------------------------------------------------

def read_csv():
    # Read CSV file
    try:
        df = pd.read_csv(CSV_FILE, dtype=str)
        return df
    except Exception as e:
        print(f"Unexpected error reading '{CSV_FILE}': {e}")
        raise


def connect_to_db():
    try:
        # Connect to Database
        print("Connecting to Database...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connection successful")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise


def create_table_ojects(conn):
    cur = conn.cursor()

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

    try:
        print("Creating schema and table if not exists")
        cur.execute(create_table_sql)
        conn.commit()
        print("Table creation successful")

    except psycopg2.Error as e:
        print(f"PSQL execution error: {e}")
        raise

    finally:
        cur.close()


def insert_records(conn,records):
    cur = conn.cursor()
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
    try: 
        print("Inserting records")
        execute_batch(cur, insert_sql, records, page_size=5000)
        conn.commit()
        print(f"Successfully inserted {len(records)} rows into {TARGET_TABLE}")

    except psycopg2.Error as e:
        print(f"PSQL execution error: {e}")
        raise

    finally:
        cur.close()


def main():
    # Strip whitespace and fill NaN with None
    df = read_csv()
    if not df.empty:
        df = df.map(lambda x: str(x).strip() if pd.notnull(x) else None)
        print(f"Loaded {len(df)} rows from {CSV_FILE}")

        # Convert DataFrame to list of tuples
        records = [tuple(row) for row in df.to_numpy()]

        try:
            conn = connect_to_db()
            create_table_ojects(conn)
            insert_records(conn,records)
        except Exception as e:
            print(f"An error occured: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
    else:
        print(f"Source CSV {CSV_FILE} is empty. Nothing to do")
