import logging
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DWH_RATING.lecturer",
    schedule=[Dataset("ODS_RATING.lecturer")],
    tags=["dwh", "core", "rating", "comment"],
    description="scd2_lecturer_hist",
    start_date=datetime(2024, 11, 3),
    catchup=False,
    default_args={
        "retries": 1,
        "owner": "mixx3",
    },
):
    PostgresOperator(
        task_id="lecturer_hist",
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            """
        -- close records
        update "DWH_RATING".lecturer as lecturer
        set valid_to_dt = '{{ ds }}'::Date
        where lecturer.api_id NOT IN(
            select dwh.api_id from
                (select
                    api_id,
                    first_name,
                    last_name,
                    middle_name,
                    subject,
                    avatar_link,
                    timetable_id
                from "DWH_RATING".lecturer
                ) as dwh
            join "ODS_RATING".lecturer as ods
            on  dwh.api_id = ods.api_id
            and dwh.first_name = ods.first_name
            and dwh.last_name = ods.last_name
            and dwh.middle_name = ods.middle_name
            and dwh.subject is not distinct from ods.subject
            and dwh.avatar_link = ods.avatar_link
            and dwh.timetable_id = ods.timetable_id
        );

        --evaluate increment
        insert into "DWH_RATING".lecturer
        select 
            ods.*,
            '{{ ds }}'::Date,
            null
            from "ODS_RATING".lecturer as ods
            full outer join "DWH_RATING".lecturer as dwh
              on ods.api_id = dwh.api_id
            where 
              dwh.api_id is NULL
              or dwh.valid_to_dt='{{ ds }}'::Date
        LIMIT 1000000; -- чтобы не раздуло
        """
        ),
        inlets=[Dataset("ODS_RATING.lecturer")],
        outlets=[Dataset("DWH_RATING.lecturer")],
    )
