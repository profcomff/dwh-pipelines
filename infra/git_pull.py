from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="git_pull_pipelines",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["infra"],
    default_args={"owner": "infra", "retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:
    BashOperator(
        task_id="git_pull",
        bash_command=f"cd /airflow/dags/dwh-pipelines && git fetch && git reset --hard origin/main && git submodule update --init --recursive",
    )
