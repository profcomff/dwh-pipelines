from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_RATING.dm_lecturer_comment_act",
    schedule=[
        Dataset("DWH_RATING.comment"),
        Dataset("ODS_RATING.lecturer_user_comment"),
    ],
    tags=["dm", "rating", "comment"],
    start_date=datetime(2024, 11, 3),
    catchup=False,
    default_args={
        "owner": "mixx3",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    PostgresOperator(
        task_id="build_cdm",
        postgres_conn_id="postgres_dwh",
        sql="dm_lecturer_comment_act.sql",
        inlets=[
            Dataset("DWH_RATING.comment"),
            Dataset("DWH_RATING.lecturer"),
            Dataset("ODS_RATING.lecturer_user_comment"),
            Dataset("DWH_USER_INFO.info"),
        ],
        outlets=[Dataset("DM_RATING.dm_lecturer_comment_act")],
    )
