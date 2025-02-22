import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG



with DAG(
    dag_id = 'DWH_RATING.comment',
    schedule=[Dataset("ODS_RATING.comment")],
    tags=["dwh", "core", "rating", "comment"],
    start_date = datetime(2024, 11, 3),
    catchup=False,
    description='scd2_comment_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='comment_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        -- close records
        update "DWH_RATING".comment as comment
        set valid_to_dt = '{{ ds }}'::Date
        where comment.uuid NOT IN(
            select dwh.uuid from
                (select 
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
                from "DWH_RATING".comment
                ) as dwh
            join "ODS_RATING".comment as ods
            on dwh.uuid = ods.uuid
            and dwh.api_uuid = ods.api_uuid
            and dwh.create_ts = ods.create_ts
            and dwh.update_ts = ods.update_ts
            and dwh.subject = ods.subject
            and dwh.text = ods.text
            and dwh.mark_kindness = ods.mark_kindness
            and dwh.mark_freebie = ods.mark_freebie
            and dwh.mark_clarity = ods.mark_clarity
            and dwh.lecturer_id = ods.lecturer_id
            and dwh.review_status = ods.review_status
        );

        --evaluate increment
        insert into "DWH_RATING".comment
        select 
            ods.*,
            '{{ ds }}'::Date,
            null
            from "ODS_RATING".comment as ods
            full outer join "DWH_RATING".comment as dwh
              on ods.uuid = dwh.uuid
            where 
              dwh.uuid is NULL
              or dwh.valid_to_dt='{{ ds }}'::Date
        LIMIT 10000000; -- чтобы не раздуло
        """),
        inlets = [Dataset("ODS_RATING.comment")],
        outlets = [Dataset("DWH_RATING.comment")],
    )
