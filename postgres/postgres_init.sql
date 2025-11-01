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


CREATE USER db_airflow PASSWORD 'db_airflow';
CREATE DATABASE airflow OWNER db_airflow;

