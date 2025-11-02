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
CREATE TABLE IF NOT EXISTS demo.raw_customer_transactions (
    transaction_id TEXT,
    customer_id TEXT,
    transaction_date TEXT,
    product_id TEXT,
    product_name TEXT,
    quantity TEXT,
    price TEXT,
    tax TEXT
);

CREATE USER db_airflow PASSWORD 'db_airflow';
CREATE DATABASE airflow OWNER db_airflow;

-- COPY demo.raw_customer_transactions(transaction_id,customer_id,transaction_date,product_id,product_name,quantity,price,tax) FROM '../data/customer_transactions.csv' DELIMITER ',' CSV HEADER;
