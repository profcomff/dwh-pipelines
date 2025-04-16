import logging
from datetime import datetime
from textwrap import dedent
import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

def get_sql_code(filename):
    directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(directory, 'info.sql')
    with open(file_path, 'r') as f:
        sql_code = f.read()
    return f"```SQL\n{sql_code}\n```"

with DAG(
    dag_id="DWH_USER_INFO.info",
    start_date=datetime(2024, 10, 1),
    schedule=[Dataset("STG_USERDATA.info"), Dataset("STG_USERDATA.param")],
    catchup=False,
    tags=["dwh", "core", "user_info"],
    description="union_members_data_format_correction",
    default_args={
        "retries": 1,
        "owner": "redstoneenjoyer",
    },
) as dag:
    run_sql = PostgresOperator(
        task_id="execute_sql",
        postgres_conn_id="postgres_dwh",
        sql="info.sql",
        doc_md=get_sql_code('info.sql'),
        inlets=[Dataset("STG_USERDATA.info"), Dataset("STG_USERDATA.param")],
        outlets=[Dataset("DWH_USER_INFO.info")],
    )
