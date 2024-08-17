from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="upload_groups_from_api",
    start_date=datetime(2024, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["dwh", "dm", "timetable", "group"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_group_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_group_act (
                group_api_id,
                group_name_text,
                group_number,
                source_name
            )
            select
                    id as group_api_id,
                    number || name as group_name_text,
                    cast(number as int) as group_number,
                    'profcomff_timetable_api' as source_name,
                from "STG_TIMETABLE"."group"
            where not is_deleted
        """),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.group")],
        outlets=[Dataset("DM_TIMETABLE.dim_group_act")],
    )


with DAG(
    dag_id="upload_events_from_api",
    start_date=datetime(2024, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["dwh", "dm", "timetable", "event"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_event_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_event_act (
                event_name_text,
                source_name
            )
            select
                name as event_name_text,
                'profcomff_timetable_api' as source_name
            from "STG_TIMETABLE"."event"
            where not is_deleted
        """),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.event")],
        outlets=[Dataset("DM_TIMETABLE.dim_event_act")],
    )

with DAG(
    dag_id="upload_lecturers_from_api",
    start_date=datetime(2024, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["dwh", "dm", "timetable", "lecturer"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_lecturer_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_lecturer_act (
                lecturer_api_id,
                lecturer_first_name,
                lecturer_middle_name,
                lecturer_last_name,
                lecturer_avatar_id,
                lecturer_description,
                source_name
            )
            select
                id as lecturer_api_id,
                first_name,
                middle_name,
                last_name,
                avatar_id,
                description,
                'profcomff_timetable_api' as source_name
                from "STG_TIMETABLE"."lecturer"
            where not is_deleted
        """),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.lecturer")],
        outlets=[Dataset("DM_TIMETABLE.dim_lecturer_act")],
    )

with DAG(
    dag_id="upload_rooms_from_api",
    start_date=datetime(2024, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["dwh", "dm", "timetable", "room"],
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
