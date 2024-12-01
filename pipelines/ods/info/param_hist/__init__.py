import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG


with DAG(
    dag_id = 'ODS_INFO.param_hist',
    start_date = datetime(2024, 11, 1),
    schedule=[Dataset("STG_USERDATA.param")],
    catchup=False,
    tags=["ods", "src", "userdata"],
    description='scd2_info_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='param_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        update "ODS_INFO".param_hist as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id NOT IN(
            select ods.id from
                (select 
                    id,
                    name,
                    category_id,
                    is_required,
                    changeable,
                    type,
                    create_ts,
                    modify_ts,
                    is_deleted,
                    validation
                from "ODS_INFO".param_hist
                ) as ods
            join "STG_USERDATA".param as stg
            on ods.id = stg.id
            and ods.name = stg.name
            and ods.category_id = stg.category_id
            and ods.is_required = stg.is_required
            and ods.changeable = stg.changeable
            and ods.type = stg.type
            and ods.create_ts = stg.create_ts
            and ods.modify_ts = stg.modify_ts
            and ods.is_deleted = stg.is_deleted
            and ods.validation = stg.validation
        );

        --evaluate increment
        insert into "ODS_INFO".param_hist
        select 
            stg.id,
            stg.name,
            stg.category_id,
            stg.is_required,
            stg.changeable,
            stg.type,
            stg.create_ts,
            stg.modify.ts,
            stg.is_deleted,
            stg.validation,
            '{{ ds }}'::Date,
            null
            from "STG_USERDATA".param as stg
            full outer join "ODS_INFO".param_hist as ods
              on stg.id = ods.id
            where 
              ods.id is NULL
              or ods.valid_to_dt='{{ ds }}'::Date
        LIMIT 100000; -- чтобы не раздуло
        """),
        inlets = [Dataset("STG_USERDATA.param")],
        outlets = [Dataset("ODS_INFO.param_hist")],
    )
