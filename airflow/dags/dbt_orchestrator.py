import sys
from  datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from docker.types import Mount

sys.path.append('/opt/airflow/scripts')

import extract_data

default_args = {
    'description': 'DAG to orchestrate the data pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'catchup': False,

}

dag = DAG(
    dag_id='dbt-orchestrator',
    default_args=default_args,
    schedule=None,

)

with dag:

    task1 = DockerOperator(
        task_id='transform_data',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='build',
        working_dir='/usr/app',
        mounts=[      
            Mount(
                source='/home/bern/repo_wsl/cx-txn-demo/dbt/cx_txn_dbt',
                target='/usr/app',
                type='bind'),
            Mount(
                source='/home/bern/repo_wsl/cx-txn-demo/dbt',
                target='/root/.dbt/]',
                type='bind'),
        ],
        network_mode='cx-txn-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'

    )

    task2 = PythonOperator(
        task_id='ingest_csv',
        python_callable=extract_data.main
    )
