from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="ODS_RATING.lecturer_user_comment",
    schedule=[Dataset("STG_RATING.lecturer_user_comment")],
    start_date=datetime(2024, 11, 3),
    catchup=False,
    tags=["ods", "core", "rating", "lecturer_user_comment"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="lecturer_user_comment.sql",
        task_id="execute_query",
        inlets=[Dataset("STG_RATING.lecturer_user_comment")],
        outlets=[Dataset("ODS_RATING.lecturer_user_comment")],
    )
