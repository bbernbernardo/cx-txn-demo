# 🧱 CX Transaction Demo

> **Case study data pipeline** using **Apache Airflow**, **dbt**, and **PostgreSQL** to ingest, transform, and expose transactional data.

---

## ⚠️ Project Status

> **NOTE:** This is an **incomplete demo**.  
> The Airflow DAG currently fails to orchestrate the full data pipeline, although individual components (Airflow, dbt, and PostgreSQL) work correctly when run locally.

---

## Quick Start

Start all containers (Airflow, dbt, and PostgreSQL):

```bash
docker-compose up
```

This command will:
- Start the **Airflow** and **PostgreSQL** containers.
- Initialize and run the **dbt** build for setup.

---

## Components

The project runs under the `cx-txn-db` Docker network and includes:

| Container | Description |
|------------|--------------|
| **cx_txn_db** | PostgreSQL database |
| **dbt** | dbt transformation and testing |
| **airflow** | Apache Airflow orchestration |

---

## Pipeline Flow

1. **Extraction**
   - `extract_data.py` extracts a CSV file and inserts records into the `demo.raw_customer_transactions` table.
   - All fields are stored as **strings** to handle inconsistent values.

2. **Transformation (dbt)**
   - **Source**: Reads directly from `demo.raw_customer_transactions`.
   - **Staging**:
     - Adds technical fields.
     - Splits valid and invalid records into:
       - `demo.stg_customer_transactions`
       - `demo.int_customer_transactions_invalid`
   - **Marts**:
     - Builds **dimension** and **fact** tables.
   - **Testing**:
     - Uses dbt’s built-in generic schema tests.

3. **Orchestration (Airflow)**
   - DAGs defined for:
     - CSV Extraction  
     - dbt Build  
     - dbt Test  
     - dbt Full Refresh  
     - dbt Incremental Run  

---

## Future Improvements / TODOs

### 1. Security
- Replace hard-coded credentials with **environment variables**.
- Integrate **secrets management** (e.g., Vault, AWS Secrets Manager).

### 2. High Availability (HA)
- Separate Airflow services: **DB**, **API Server**, and **Worker**.
- Add **health checks** and improve service dependencies.
- Persist PostgreSQL data volumes.

### 3. dbt Enhancements
- Implement **Data Vault** modeling (dependencies and structure already set up).
- Add **retry logic** and **enhanced logging**.
- Integrate **dbt-expectations** for data quality tests.
- Include `dbt init`, `dbt seed`, and `dbt deps` workflows.

### 4. Presentation Layer
- Create business logic datasets for reporting.
- Add **visualization** layer (e.g., Metabase, Superset, or Tableau).

### 5. Airflow Improvements
- Fix DAG orchestration issues.
- Improve **task dependencies**, add **retries**, and enable **logging/notifications**.

---

## Tech Stack

- **Apache Airflow**
- **dbt**
- **PostgreSQL**
- **Docker Compose**
- **Python 3**
