import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


sys.path.append('opt/airflow/scripts')

def safe_main_collable():
    from insert_data import main
    return main()

defaule_args = {
    'description': 'DAG to orchestrate the data pipeline',
    'depends_on_past': False,
    'start_date': days_ago(1),

}

dag = DAG(
    dag_id='demo-cx-txn',
    default_args=default_args,
    schedule_interval=None,

)

with dag:
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=safe_main_collable
    )