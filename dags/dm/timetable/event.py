from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="DM_TIMETABLE.dim_event_act__from_api",
    start_date=datetime(2024, 11, 1),
    schedule=[Dataset("STG_TIMETABLE.event")],
    catchup=False,
    tags=["cdm", "core", "timetable_event", "timetable_api"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="event.sql",
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.event")],
        outlets=[Dataset("DM_TIMETABLE.dim_event_act")],
    )
