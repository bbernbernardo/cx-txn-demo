import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import execute_batch
from extract_data import extract_csv
from datetime import date, datetime

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'cx_txn',
    'user': 'db_user',
    'password': 'db_password'
}

def conn_db():
    print("Connection to Postgres DB...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise


def create_table(conn):
    print("Creating table if not exist ...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS demo;
            CREATE TABLE IF NOT EXISTS demo.customer_transactions (
                transaction_id INT,
                customer_id INT,
                transaction_date DATE,
                product_id INT,
                product_name TEXT,
                quantity INT,
                price FLOAT,
                tax FLOAT
            );
        """)
        conn.commit()
        print("Table Created")
    except psycopg2.Error as e:
        print("Failed to create table: {e}")
        raise

def is_valid_type(value, expected_type):
    """Check if a value matches the expected data type."""
    if pd.isna(value):
        return False  # Reject NaN
    try:
        if expected_type == int:
            return isinstance(value, (int, np.integer)) or str(value).isdigit()
        elif expected_type == float:
            float(value)
            return True
        elif expected_type == str:
            return isinstance(value, str)
        elif expected_type == date:
            # Try to parse valid date
            if isinstance(value, (date, datetime)):
                return True
            pd.to_datetime(value, errors='raise')
            return True
        else:
            return False
    except Exception:
        return False
        

def insert_valid_records(df, conn):
    """
    Validate DataFrame rows against schema and insert valid ones into PostgreSQL.
    Returns rejected rows as a DataFrame.
    """
    expected_schema = {
        'transaction_id': int,
        'customer_id': int,
        'transaction_date': date,
        'product_id': int,
        'product_name': str,
        'quantity': int,
        'price': float,
        'tax': float
    }

    valid_rows = []
    rejected_rows = []

    for _, row in df.iterrows():
        valid = True
        converted_row = {}

        for col, expected_type in expected_schema.items():
            val = row[col]
            if not is_valid_type(val, expected_type):
                valid = False
                break
            else:
                # Convert values to correct types for safe insertion
                if expected_type == int:
                    converted_row[col] = int(val)
                elif expected_type == float:
                    converted_row[col] = float(val)
                elif expected_type == str:
                    converted_row[col] = str(val)
                elif expected_type == date:
                    converted_row[col] = pd.to_datetime(val).date()

        if valid:
            valid_rows.append(tuple(converted_row.values()))
        else:
            rejected_rows.append(row)

    # --- Insert valid records into PostgreSQL ---
    if valid_rows:
        try:
            cursor = conn.cursor()

            sql = """
                INSERT INTO demo.customer_transactions
                (transaction_id, customer_id, transaction_date, product_id, product_name, quantity, price, tax)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            execute_batch(cursor, sql, valid_rows)
            conn.commit()
            cursor.close()
            print(f"Inserted {len(valid_rows)} valid rows into customer_transactions.")
        except psycopg2.Error as e:
            print("Failed to create table: {e}")
            raise
        finally:
            conn.close()
    else:
        print("No valid rows to insert.")

    # Return rejected rows as DataFrame
    rejected_df = pd.DataFrame(rejected_rows)
    print(f"Rejected {len(rejected_df)} rows.")
    return rejected_df


def main():
    conn = conn_db()
    df = extract_csv()
    rejected_df = insert_valid_records(df,conn)
    print(rejected_df)