import logging
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DWH_AUTH_USER.info",
    start_date=datetime(2024, 10, 1),
    schedule=[
        Dataset("DWH_USER_INFO.info"),
        Dataset("ODS_AUTH.auth_method"),
        Dataset("ODS_AUTH.user"),
    ],
    catchup=False,
    tags=["dwh", "src", "user_info"],
    description="union_members_data_format_correction",
    default_args={
        "retries": 1,
        "owner": "redstoneenjoyer",
    },
):
    PostgresOperator(
        task_id="merginng_and_inserting_into_ODS_INFO",
        postgres_conn_id="postgres_dwh",
        sql='info.sql',
        inlets=[
            Dataset("DWH_USER_INFO.info"),
            Dataset("ODS_AUTH.auth_method"),
            Dataset("ODS_AUTH.user"),
        ],
        outlets=[Dataset("DWH_AUTH_USER.info")],
    )
