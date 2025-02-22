from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="STG_RATING.lecturer",
    schedule=[Dataset("STG_RATING.lecturer")],
    catchup=False,
    tags=["ods", "core", "rating", "lecturer"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "ODS_RATING".lecturer;

            insert into "ODS_RATING".lecturer (
                uuid,
                api_id,
                first_name,
                last_name,
                middle_name,
                subject,
                avatar_link,
                timetable_id
            )
            select 
                gen_random_uuid() as uuid,
                id as api_id,
                first_name,
                last_name,
                middle_name,
                subject,
                avatar_link,
                timetable_id
            from "STG_RATING".lecturer
            limit 1000001;  
        """),
        task_id="execute_query",
        inlets=[Dataset("STG_RATING.lecturer")],
        outlets=[Dataset("ODS_RATING.lecturer")],
    )
