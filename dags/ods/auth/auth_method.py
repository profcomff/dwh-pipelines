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
        sql="auth_method.sql",
        inlets=[Dataset("STG_AUTH.auth_method")],
        outlets=[Dataset("ODS_AUTH.auth_method")],
    )
