from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="ODS_SOCIAL.viribus_chat",
    schedule=[Dataset("STG_SOCIAL.webhook_storage")],
    start_date=datetime(2024, 11, 3),
    catchup=False,
    tags=["ods", "core", "social", "telegram"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="telegram_viribus.sql",
        task_id="execute_query",
        inlets=[Dataset("STG_SOCIAL.webhook_storage")],
        outlets=[Dataset("ODS_SOCIAL.viribus_chat")],
    )
