import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG


with DAG(
    dag_id = 'ODS_INFO.info_hist',
    start_date = datetime(2024, 11, 1),
    schedule=[Dataset("STG_USERDATA.info")],
    catchup=False,
    tags=["ods", "src", "userdata"],
    description='scd2_info_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='info_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        update "ODS_INFO".info_hist as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id NOT IN(
            select ods.id from
                (select 
                    id,
                    param_id,
                    source_id,
                    owner_id,
                    value,
                    create_ts,
                    modify_ts,
                    is_deleted
                from "ODS_INFO".info_hist
                ) as ods
            join "STG_USERDATA".info as stg
            on ods.id = stg.id
            and ods.param_id = stg.param_id
            and ods.source_id = stg.source_id
            and ods.owner_id = stg.owner_id
            and ods.value = stg.value
            and ods.create_ts = stg.create_ts
            and ods.modify_ts = stg.modify_ts
            and ods.is_deleted = stg.is_deleted
        );

        --evaluate increment
        insert into "ODS_INFO".info_hist
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_USERDATA".info as stg
            full outer join "ODS_INFO".info_hist as ods
              on stg.id = ods.id
            where 
              ods.id is NULL
              or ods.valid_to_dt='{{ ds }}'::Date
        LIMIT 100000; -- чтобы не раздуло
        """),
        inlets = [Dataset("STG_USERDATA.info")],
        outlets = [Dataset("ODS_INFO.info_hist")],
    )
