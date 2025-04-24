from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_TIMETABLE.dm_timetable_act",
    start_date=datetime(2024, 12, 15),
    schedule=[
        Dataset("ODS_TIMETABLE.ods_link_timetable_room"),
        Dataset("ODS_TIMETABLE.ods_link_timetable_group"),
        Dataset("ODS_TIMETABLE.ods_link_timetable_teacher"),
        Dataset("ODS_TIMETABLE.ods_link_timetable_lesson"),
    ],
    catchup=False,
    tags=["cdm", "business", "timetable", "timetable_act"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="act.sql",
        task_id="execute_merge_statement",
        inlets=[
            Dataset("ODS_TIMETABLE.ods_link_timetable_room"),
            Dataset("ODS_TIMETABLE.ods_link_timetable_group"),
            Dataset("ODS_TIMETABLE.ods_link_timetable_teacher"),
            Dataset("ODS_TIMETABLE.ods_link_timetable_lesson"),
            Dataset("ODS_TIMETABLE.ods_timetable_act"),
            Dataset("DM_TIMETABLE.dim_event_act"),
            Dataset("DM_TIMETABLE.dim_room_act"),
            Dataset("DM_TIMETABLE.dim_group_act"),
            Dataset("DM_TIMETABLE.dim_lecturer_act"),
        ],
        outlets=[Dataset("DM_TIMETABLE.dm_timetable_act")],
    )
