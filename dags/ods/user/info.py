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
    dag_id="ODS_USER_INFO.info",
    start_date=datetime(2024, 10, 1),
    schedule=[Dataset("STG_USERDATA.info"), Dataset("STG_USERDATA.param"), Dataset("STG_UNION_MEMBER.union_member")],
    catchup=False,
    tags=["ods", "core", "user_info"],
    description="merge_userdata_union_member",
    default_args={
        "retries": 1,
        "owner": "VladislavVoskoboinik",
    },
) as dag:
    run_sql_regular = PostgresOperator(
        task_id="execute_sql",
        postgres_conn_id="postgres_dwh",
        sql="info.sql",
        doc_md=get_sql_code("info.sql", os.path.dirname(os.path.abspath(__file__))),
        # порядок датасетов здесь важен!
        # см. info.sql и https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#variables
        inlets=[Dataset("STG_USERDATA.info"), Dataset("STG_USERDATA.param"), Dataset("STG_UNION_MEMBER.union_member")],
        outlets=[Dataset("ODS_USER_INFO.info")],
        params={"tablename": "info"},
    )
    # run_sql_encrypted = PostgresOperator(
    #     task_id="execute_sql_encrypted",
    #     postgres_conn_id="postgres_dwh",
    #     sql="info.sql",
    #     doc_md=get_sql_code("info.sql", os.path.dirname(os.path.abspath(__file__))),
    #     # порядок датасетов здесь важен!
    #     # см. info.sql и https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#variables
    #     inlets=[
    #         Dataset("STG_USERDATA.encrypted_info"),
    #         Dataset("STG_USERDATA.param"),
    #         Dataset("STG_UNION_MEMBER.union_member"),
    #     ],
    #     outlets=[Dataset("ODS_USER_INFO.encrypted_info")],
    #     params={"tablename": "encrypted_info"},
    # ) TODO сделать мерж юсердаты шифрованной
