import logging
import os
from datetime import datetime
from functools import partial
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import get_sql_code


with DAG(
    dag_id="DWH_RATING.lecturer",
    schedule=[Dataset("ODS_RATING.lecturer")],
    tags=["dwh", "core", "rating", "comment"],
    description="scd2_lecturer_hist",
    start_date=datetime(2024, 11, 3),
    catchup=True,
    default_args={
        "retries": 1,
        "owner": "mixx3",
    },
):
    close_records = PostgresOperator(
        task_id="lecturer_close_records",
        postgres_conn_id="postgres_dwh",
        sql="lecturer_close_records.sql",
        doc_md=get_sql_code("lecturer_close_records.sql", os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("ODS_RATING.lecturer")],
        outlets=[Dataset("DWH_RATING.lecturer")],
    )
    evaluate_increment = PostgresOperator(
        task_id="lecturer_evaluate_increment",
        postgres_conn_id="postgres_dwh",
        sql="lecturer_evaluate_increment.sql",
        doc_md=get_sql_code("lecturer_evaluate_increment.sql", os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("ODS_RATING.lecturer")],
        outlets=[Dataset("DWH_RATING.lecturer")],
    )
    calculate_rank = PostgresOperator(
        task_id="lecturer_calculate_rank",
        postgres_conn_id="postgres_dwh",
        sql="lecturer_calculate_rank.sql",
        doc_md=get_sql_code("lecturer_calculate_rank.sql", os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("ODS_RATING.lecturer")],
        outlets=[Dataset("DWH_RATING.lecturer")],
    )
    close_records >> evaluate_increment >> calculate_rank
