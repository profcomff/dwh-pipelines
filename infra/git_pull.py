from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


with DAG(
    dag_id="git_pull_pipelines",
    start_date=datetime(2022,1,1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags= ["dwh"],
    default_args={
        "owner": "dwh",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    BashOperator(
        task_id="git_pull",
        bash_command=f"cd /root/airflow/dags/dwh-pipelines && git reset --hard origin/main"
    )