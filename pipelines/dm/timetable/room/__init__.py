from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_TIMETABLE.dim_room_act__from_api",
    start_date=datetime(2024, 11, 1),
    schedule=[Dataset("STG_TIMETABLE.room")],
    catchup=False,
    tags=["cdm", "core", "room", "timetable_api"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_room_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_room_act (
                room_direction_text_type,
                room_api_id,
                room_name,
                room_department,
                source_name
            )
            select
                direction as room_api_id,
                id as room_api_id,
                name as room_name,
                building as room_department,
                'profcomff_timetable_api' as source_name
                from "STG_TIMETABLE"."room"
            where not is_deleted
        """),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.room")],
        outlets=[Dataset("DM_TIMETABLE.dim_room_act")],
    )
