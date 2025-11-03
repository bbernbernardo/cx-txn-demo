# cx-txn-demo
Case study data pipeline using Airflow, dbt, and PostgreSQL to ingest, transform, and expose data


# !!!NOTE: Incomplete demo.
# Major issue - Airflow DAG unable to orchestrate data pipeline.
# Components work individualy and locally.

docker-compose up
# Will enable airflow and postgres containers. As well as dbt build run (initial set up)

# List of containers under cx-txn-db:
# 1. cx_txn_db - Postgres DB
# 2. dbt - dbt package
# 3. airflow - Airflow container


# Pipeline Flow
# 1. extract_data.py will extract the CSV file and insert the records to demo.raw_customer_transactions postgres table.
#       The table fields are all STRING to accommodate varying and inconsistent values.
# 2. dbt 
#       Source: Takes the raw demo.raw_customer_transactions postgres table as it is.
#       Staging: Adds technical fields and separates Valid and Invalid records. Valid records will go to demo.stg_customer_transactions, and invalid records goes to demo.int_customer_transactions_invalid
#       Marts:  dim and fact tables are created.
#       Test: Leverages on built in generic test in schema definition.
# 3. Airflow. Dags created for
#       CSV Extraction
#       dbt execution of Buil, Test, Full-Refresh, and Run incremental.



# Lots of TO DOs
# 1. Security Improvement:
#       a. Remove hard coded sensitive information and utilize env varialbes for variable definition e.g. db users, password, environments.
#       b. Add secrets management
# 2. HA:
#       a. Separate Airflow services: DB, API Server and Workflow
#       b. Improve services dependencies by adding helthchecks.
#       c. Persist postgress data
# 3. dbt:
#       a. Improve data modeling by implementing Data Vault. Dependencies have been installed, and folder structures have been set up.
#       b. Implement retries and Logging.
#       c. Improve quality checks by adding dbt_expectations.
#       d. Add dbt init like seed and dbt deps.
# 4. Presentation layer - add dataset models for business logics and add visualization on top.
# 5. Airflow
#       a. Firstly, fix dags issues.
#       b. Improve tasks dependencies and add retries.
#       c. Add logging and notification

