from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_TIMETABLE.dim_lecturer_act__from_api",
    start_date=datetime(2024, 11, 1),
    schedule=[Dataset("STG_TIMETABLE.lecturer")],
    catchup=False,
    tags=["cdm", "core", "lecturer", "timetable_api"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_lecturer_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_lecturer_act (
                id,
                lecturer_api_id,
                lecturer_first_name,
                lecturer_middle_name,
                lecturer_last_name,
                lecturer_avatar_id,
                lecturer_description,
                source_name
            )
            select
                gen_random_uuid(),
                min(id) as lecturer_api_id,
                first_name,
                middle_name,
                last_name,
                min(avatar_id),
                min(description),
                'profcomff_timetable_api' as source_name
                from "STG_TIMETABLE"."lecturer"
            where not is_deleted
            group by first_name, middle_name, last_name
            order by lecturer_api_id
        """
        ),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.lecturer")],
        outlets=[Dataset("DM_TIMETABLE.dim_lecturer_act")],
    )

with DAG(
    dag_id="DM_TIMETABLE.dim_lecturer_act__from_dubinushka",
    start_date=datetime(2024, 11, 9),
    schedule=[Dataset("STG_DUBINUSHKA_MANUAL.lecturer")],
    catchup=False,
    tags=["cdm", "core", "lecturer", "dubinushka"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="lecturer.sql",
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_DUBINUSHKA_MANUAL.lecturer")],
        outlets=[Dataset("DM_TIMETABLE.dim_lecturer_act")],
    )
