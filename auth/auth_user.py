import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG



with DAG(
    dag_id = 'ODS_AUTH.user',
    start_date = datetime(2024, 11, 3),
    schedule=[Dataset("STG_AUTH.user")],
    catchup=False,
    tags=["ods", "src", "auth"],
    description='scd2_user_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='user_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        -- close records
        update "ODS_AUTH".user as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id NOT IN(
            select ods.id from
                (select 
                    id,
                    create_ts,
                    update_ts,
                    is_deleted
                from "ODS_AUTH".user
                ) as ods
            join "STG_AUTH".user as stg
            on ods.id = stg.id
            and (
                ods.create_ts = stg.create_ts
                or ods.update_ts = stg.update_ts
                or ods.is_deleted = stg.is_deleted
            )
        );

        --evaluate increment
        insert into "ODS_AUTH".user
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_AUTH".user as stg
            full outer join "ODS_AUTH".user as ods
              on stg.id = ods.id
            where 
              ods.id is NULL
              or stg.id is NULL
              or ods.valid_to_dt='{{ ds }}'::Date
              LIMIT 100000; -- чтобы не раздуло
        ;
        """),
        inlets = [Dataset("STG_AUTH.user")],
        outlets = [Dataset("ODS_AUTH.user")],
    )


with DAG(
    dag_id = 'ODS_AUTH.auth_method',
    start_date = datetime(2024, 11, 3),
    schedule=[Dataset("STG_AUTH.auth_method")],
    catchup=False,
    tags=["ods", "src", "auth"],
    description='scd2_auth_method_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='auth_method_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        -- close records
        update "ODS_AUTH".auth_method as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id NOT IN(
            select ods.id from
                (select 
                    id,
                    user_id,
                    auth_method,
                    param,
                    value,
                    create_ts,
                    update_ts,
                    is_deleted
                from "ODS_AUTH".auth_method
                ) as ods
            join "STG_AUTH".auth_method as stg
            on ods.id = stg.id 
            and (
                ods.user_id = stg.user_id
                or ods.auth_method = stg.auth_method
                or ods.param = stg.param
                or ods.value = stg.value
                or ods.create_ts = stg.create_ts
                or ods.update_ts = stg.update_ts
                or ods.is_deleted = stg.is_deleted
            )
        );

        --evaluate increment
        insert into "ODS_AUTH".auth_method
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_AUTH".auth_method as stg
            full outer join "ODS_AUTH".auth_method as ods
              on stg.id = ods.id
            where 
              ods.id is NULL
              or stg.id is NULL
              or ods.valid_to_dt='{{ ds }}'::Date
              LIMIT 100000; -- чтобы не раздуло
        ;
        """),
        inlets = [Dataset("STG_AUTH.auth_method")],
        outlets = [Dataset("ODS_AUTH.auth_method")],
    )
