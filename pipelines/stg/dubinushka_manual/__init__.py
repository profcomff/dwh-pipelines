import os
from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with DAG(
    dag_id="STG_DUBINUSHKA_MANUAL.lecturer",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["stg", "src", "dubinushka", "lecturer"],
    default_args={"owner": "mixx3"},
):
    with open(f"{CUR_DIR}/lecturers.sql", "r") as sql_query_file:
        PostgresOperator(
            postgres_conn_id="postgres_dwh",
            sql=sql_query_file.read(),
            task_id="upload_lecturers_from_backup",
            outlets=[Dataset("STG_DUBINUSHKA_MANUAL.lecturer")],
        )

with DAG(
    dag_id="STG_DUBINUSHKA_MANUAL.comment",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["stg", "src", "dubinushka", "lecturer"],
    default_args={"owner": "mixx3"},
):
    with open(f"{CUR_DIR}/comments.sql", "r") as sql_query_file:
        PostgresOperator(
            postgres_conn_id="postgres_dwh",
            sql=sql_query_file.read(),
            task_id="upload_comments_from_backup",
            outlets=[Dataset("STG_DUBINUSHKA_MANUAL.comment")],
        )
