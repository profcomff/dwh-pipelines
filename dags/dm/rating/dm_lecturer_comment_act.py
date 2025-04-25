from datetime import datetime, timedelta
from functools import partial
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import alert_message


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
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
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
