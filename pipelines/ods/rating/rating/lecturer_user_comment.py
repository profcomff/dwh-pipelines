from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="STG_RATING.lecturer_user_comment",
    schedule=[Dataset("STG_RATING.lecturer_user_comment")],
    catchup=False,
    tags=["ods", "core", "rating", "lecturer_user_comment"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "ODS_RATING".lecturer_user_comment;

            insert into "ODS_RATING".lecturer_user_comment (
                uuid,
                api_id,
                lecturer_id,
                user_id,
                create_ts,
                update_ts
            )
            select 
                gen_random_uuid() as uuid,
                id as api_id,
                lecturer_id,
                user_id,
                create_ts at time zone 'utc' at time zone 'Europe/Moscow' as create_ts,
                update_ts at time zone 'utc' at time zone 'Europe/Moscow' as update_ts
            from "STG_RATING".lecturer_user_comment
            limit 1000001;
        """),
        task_id="execute_query",
        inlets=[Dataset("STG_RATING.lecturer_user_comment")],
        outlets=[Dataset("ODS_RATING.lecturer_user_comment")],
    )
