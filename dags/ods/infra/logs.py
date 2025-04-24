from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="ODS_INFRA.logs",
    start_date=datetime(2024, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["dwh", "ods", "infra", "logs"],
    default_args={"owner": "dyakovri"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="logs.sql",
        task_id="execute_insert_statement",
        inlets=[Dataset("STG_INFRA.container_log")],
        outlets=[Dataset("ODS_INFRA_LOGS.container_log")],
    )
