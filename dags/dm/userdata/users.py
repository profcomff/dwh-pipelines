from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="DM_USER.unionmembers_join_with_users",
    schedule=[Dataset("DWH_USER_INFO.info"), Dataset("STG_UNION_MEMBER.union_member")],
    start_date=datetime(2024, 8, 27),
    tags=["dm", "src", "userdata"],
    default_args={
        "owner": "wudext",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    PostgresOperator(
        task_id="make_join",
        postgres_conn_id="postgres_dwh",
        sql="users.sql",
        inlets=[
            Dataset("DWH_USER_INFO.info"),
            Dataset("STG_UNION_MEMBER.union_member"),
        ],
        outlets=[Dataset("DM_USER.union_member_join")],
    )
