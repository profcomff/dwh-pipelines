from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_MARKETING.frontend_actions_services",
    start_date=datetime(2025, 4, 10),
    schedule=[Dataset("ODS_MARKETING.frontend_actions")],
    catchup=False,
    tags=["dm", "marketing"],
    default_args={
        "owner": "VladislavVoskoboinik",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
):
    PostgresOperator(
        task_id="frontend_moves",
        postgres_conn_id="postgres_dwh",
        sql="frontend_actions_services.sql",
        inlets=[
            Dataset("STG_SERVICES.button"),
            Dataset("ODS_MARKETING.frontend_actions"),
        ],
        outlets=[Dataset("DM_marketing.frontend_actions_services")],
    )
