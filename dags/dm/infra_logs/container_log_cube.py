from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="logs_cube",
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("ODS_INFRA_LOGS.container_log")],
    catchup=False,
    tags=["dwh", "dm", "infra", "logs"],
    default_args={"owner": "dyakovri"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="container_log_cube.sql",
        task_id="execute_merge_statement",
        inlets=[Dataset("ODS_INFRA_LOGS.container_log")],
        outlets=[Dataset("DM_INFRA_LOGS.container_log_cube")],
    )
