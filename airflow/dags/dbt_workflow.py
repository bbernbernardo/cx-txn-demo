import sys
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
sys.path.append('/opt/airflow/scripts')

import extract_data
# DAG definition
default_args = {
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='dbt_workflow',
    default_args=default_args,
    description='Run dbt tasks from Airflow',
    start_date=datetime(2025, 11, 3),
    schedule=None,  # manually triggered
    catchup=False,
    tags=['dbt', 'etl'],
) as dag:

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='docker exec dbt bash -c "dbt build"',
        dag=dag
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /usr/app && dbt test'
    )

    dbt_full_refresh = BashOperator(
        task_id='dbt_full_refresh',
        bash_command='cd /usr/app && dbt run --full-refresh'
    )


    # TO DO: Add selector. There is a selector but for some reason, not working.
    dbt_incremental = BashOperator(
        task_id='dbt_incremental',
        bash_command='cd /usr/app && dbt run'
    )

    
    # ingest_csv = PythonOperator(
    #     task_id='ingest_csv',
    #     python_callable=extract_data.main
    # )
