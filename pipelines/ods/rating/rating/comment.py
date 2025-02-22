from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="STG_RATING.comment",
    schedule=[Dataset("STG_RATING.comment")],
    start_date = datetime(2024, 11, 3),
    catchup=False,
    tags=["ods", "core", "rating", "comment"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "ODS_RATING".comment;

            insert into "ODS_RATING".comment (
                uuid,
                api_uuid,
                create_ts,
                update_ts,
                subject,
                text,
                mark_kindness,
                mark_freebie,
                mark_clarity,
                lecturer_id,
                review_status
            )
            select 
                gen_random_uuid() as uuid,
                uuid as api_uuid,
                create_ts at time zone 'utc' at time zone 'Europe/Moscow' as create_ts,
                update_ts at time zone 'utc' at time zone 'Europe/Moscow' as update_ts,
                subject,
                text,
                mark_kindness,
                mark_freebie,
                mark_clarity,
                lecturer_id,
                review_status
            from "STG_RATING".comment
            limit 1000001;  
        """),
        task_id="execute_query",
        inlets=[Dataset("STG_RATING.comment")],
        outlets=[Dataset("ODS_RATING.comment")],
    )
