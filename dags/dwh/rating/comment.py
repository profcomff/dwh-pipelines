import os
import logging
from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import get_sql_code

with DAG(
    dag_id="DWH_RATING.comment",
    schedule=[Dataset("ODS_RATING.comment")],
    tags=["dwh", "core", "rating", "comment"],
    start_date=datetime(2024, 11, 3),
    catchup=False,
    description="scd2_comment_hist",
    default_args={
        "retries": 1,
        "owner": "mixx3",
    },
):
    PostgresOperator(
        task_id="comment_hist",
        postgres_conn_id="postgres_dwh",
        sql="comment.sql",
        doc_md=get_sql_code('comment.sql', os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("ODS_RATING.comment")],
        outlets=[Dataset("DWH_RATING.comment")],
    )
