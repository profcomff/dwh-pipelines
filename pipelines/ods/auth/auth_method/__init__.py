import logging
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="ODS_AUTH.auth_method",
    start_date=datetime(2024, 11, 3),
    schedule=[Dataset("STG_AUTH.auth_method")],
    catchup=False,
    tags=["ods", "src", "auth"],
    description="scd2_auth_method_hist",
    default_args={
        "retries": 1,
        "owner": "mixx3",
    },
):
    PostgresOperator(
        task_id="auth_method_hist",
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            """
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
            and ods.user_id = stg.user_id
            and ods.auth_method = stg.auth_method
            and ods.param = stg.param
            and ods.value = stg.value
            and ods.create_ts = stg.create_ts
            and ods.update_ts = stg.update_ts
            and ods.is_deleted = stg.is_deleted
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
              or ods.valid_to_dt='{{ ds }}'::Date
        LIMIT 100000; -- чтобы не раздуло
        """
        ),
        inlets=[Dataset("STG_AUTH.auth_method")],
        outlets=[Dataset("ODS_AUTH.auth_method")],
    )
