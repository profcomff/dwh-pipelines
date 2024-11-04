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
        task_id='info_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        -- close records
        update "ODS_AUTH".user as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id=any(
            select ods.id from
                (select 
                    id,
                    create_ts,
                    update_ts,
                    is_deleted,
                from "ODS_AUTH".user
                ) as ods
            join "STG_AUTH".user as stg
            on md5(ods::text) != md5(stg::text)
        );

        --evaluate increment
        insert into "ODS_AUTH".user
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_AUTH".user as stg
            join "ODS_AUTH".user as dst
            on stg.id != dst.id or dst.valid_from_dt is not null
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
        task_id='param_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        -- close records
        update "ODS_AUTH".auth_method as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id=any(
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
            on md5(ods::text) != md5(stg::text)
        );

        --evaluate increment
        insert into "ODS_AUTH".auth_method
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_AUTH".auth_method as stg
            join "ODS_AUTH".auth_method as dst
            on stg.id != dst.id or dst.valid_from_dt is not null
        ;
        """),
        inlets = [Dataset("STG_AUTH.auth_method")],
        outlets = [Dataset("ODS_AUTH.auth_method")],
    )
