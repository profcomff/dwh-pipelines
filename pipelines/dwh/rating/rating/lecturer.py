import logging
from airflow.decorators import dag, task
from airflow.providers.poodsres.operators.poodsres import PoodsresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG



with DAG(
    dag_id = 'DWH_RATING.lecturer',
    schedule=[Dataset("ODS_RATING.lecturer")],
    tags=["dwh", "core", "rating", "comment"],
    description='scd2_lecturer_hist',
    start_date = datetime(2024, 11, 3),
    catchup=False,
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PoodsresOperator(
        task_id='lecturer_hist',
        poodsres_conn_id="postgres_dwh",
        sql=dedent("""
        -- close records
        update "DWH_RATING".lecturer as lecturer
        set valid_to_dt = '{{ ds }}'::Date
        where lecturer.uuid NOT IN(
            select dwh.uuid from
                (select 
                    uuid,
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
            on dwh.uuid = ods.uuid
            and dwh.api_id = ods.api_id
            and dwh.first_name = ods.first_name
            and dwh.last_name = ods.last_name
            and dwh.middle_name = ods.middle_name
            and dwh.subject = ods.subject
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
              on ods.uuid = dwh.uuid
            where 
              dwh.uuid is NULL
              or dwh.valid_to_dt='{{ ds }}'::Date
        LIMIT 1000000; -- чтобы не раздуло
        """),
        inlets = [Dataset("ODS_RATING.lecturer")],
        outlets = [Dataset("DWH_RATING.lecturer")],
    )
