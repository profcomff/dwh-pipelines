import logging
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_MONITORING.db_monitoring_snp",
    start_date=datetime(2024, 11, 10),
    schedule="@daily",
    catchup=False,
    tags=["ods", "src", "userdata"],
    description="data weight monitoring",
    default_args={
        "retries": 1,
        "owner": "mixx3",
    },
):
    PostgresOperator(
        task_id="dm_monitoring",
        postgres_conn_id="postgres_dwh",
        sql="db_monitoring_snp.sql",
        inlets=[],
        outlets=[Dataset("DM_MONITORING.db_monitoring_snp")],
    )
