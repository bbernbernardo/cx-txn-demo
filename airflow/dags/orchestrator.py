import sys
from  datetime import datetime
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator


sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/data')

import extract_data


default_args = {
    'description': 'DAG to orchestrate the data pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'catchup': False,

}

dag = DAG(
    dag_id='demo-cx-txn',
    default_args=default_args,
    schedule=None,

)

with dag:
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=extract_data.main()
    )
