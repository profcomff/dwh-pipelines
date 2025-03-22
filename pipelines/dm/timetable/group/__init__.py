from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_TIMETABLE.dim_group_act__from_api",
    start_date=datetime(2024, 11, 1),
    schedule=[Dataset("STG_TIMETABLE.group")],
    catchup=False,
    tags=["cdm", "core", "group", "timetable_api"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            r"""
            -- truncate old state
            delete from "DM_TIMETABLE".dim_group_act
            where source_name = 'profcomff_timetable_api';

            insert into "DM_TIMETABLE".dim_group_act (
                id,
                group_api_id,
                group_name_text,
                group_number,
                source_name
            )
            select
                gen_random_uuid(),
                min(id) as group_api_id,
                number || name as group_name_text,
                number as group_number,
                'profcomff_timetable_api' as source_name
            from "STG_TIMETABLE"."group"
            where not is_deleted
            group by group_name_text, group_number
            order by group_api_id
        """
        ),
        task_id="execute_merge_statement",
        inlets=[Dataset("STG_TIMETABLE.group")],
        outlets=[Dataset("DM_TIMETABLE.dim_group_act")],
    )
