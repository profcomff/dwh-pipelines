import logging
import os
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import get_sql_code

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
        sql="lecturer.sql",
        doc_md=get_sql_code("lecturer.sql", os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("ODS_RATING.lecturer")],
        outlets=[Dataset("DWH_RATING.lecturer")],
    )
