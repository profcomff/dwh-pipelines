from datetime import datetime
from functools import partial
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import alert_message


with DAG(
    dag_id="ODS_RATING.comment",
    schedule=[Dataset("STG_RATING.comment")],
    start_date=datetime(2024, 11, 3),
    catchup=False,
    tags=["ods", "core", "rating", "comment"],
    default_args={
        "owner": "mixx3",
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
    },
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="comment.sql",
        task_id="execute_query",
        inlets=[Dataset("STG_RATING.comment")],
        outlets=[Dataset("ODS_RATING.comment")],
    )
