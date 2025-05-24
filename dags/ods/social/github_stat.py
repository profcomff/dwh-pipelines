from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="ODS_SOCIAL.github_stat",
    schedule=[Dataset("STG_SOCIAL.webhook_storage")],
    start_date=datetime(2025, 5, 22),
    catchup=False,
    tags=["ods", "social", "github"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="github_stat.sql",
        task_id="execute_query",
        inlets=[Dataset("STG_SOCIAL.webhook_storage")],
        outlets=[Dataset("ODS_SOCIAL.git_hub")],
    )
