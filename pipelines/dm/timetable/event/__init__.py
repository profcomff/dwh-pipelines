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
        sql=dedent(
            r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_event_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_event_act (
                id,
                event_name_text,
                source_name,
                event_api_id
            )
            select
                gen_random_uuid(),
                name as event_name_text,
                'profcomff_timetable_api' as source_name,
                min(id) as event_api_id
            from "STG_TIMETABLE"."event"
            where not is_deleted
            group by name
            order by event_api_id
        """
        ),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.event")],
        outlets=[Dataset("DM_TIMETABLE.dim_event_act")],
    )
