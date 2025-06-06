from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="ODS_TIMETABLE.ods_link_timetable_teacher",
    start_date=datetime(2024, 12, 7),
    schedule=[Dataset("DM_TIMETABLE.dim_lecturer_act")],
    catchup=False,
    tags=["ods", "core", "timetable", "link_timetable_teacher"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="link_timetable_lecturer.sql",
        task_id="execute_query",
        inlets=[
            Dataset("ODS_TIMETABLE.ods_timetable_act"),
            Dataset("DM_TIMETABLE.dim_lecturer_act"),
        ],
        outlets=[Dataset("ODS_TIMETABLE.ods_link_timetable_teacher")],
    )
