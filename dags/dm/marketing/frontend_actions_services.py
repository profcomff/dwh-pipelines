from datetime import datetime, timedelta
from functools import partial
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import alert_message


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
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
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
