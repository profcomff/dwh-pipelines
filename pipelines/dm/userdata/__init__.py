import json
import logging
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from sqlalchemy import create_engine
import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable

DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)

@task(
    task_id="make_join",
    inlets=Dataset("DWH_USER.union_member", "DWH_USER.info"),
    outlets=Dataset("DM_USER.union_member_join")
)
def make_join():
    sql_engine = sa.create_engine(DB_DSN)
    sql_engine.execute(
        """
        INSERT INTO "DM_USER".union_member_join
        SELECT 
            t1.*,
            t2.card_status,
            t2.card_date,
            t2.card_number
        FROM 
            "DWH_USER".info AS t1
        INNER JOIN 
            "DWH_USER".union_member AS t2
        ON 
            t1.full_name = t2.full_name;
        """
    )
    return Dataset("DM_USER.union_member_join")


with DAG(
    dag_id="users_unionmembers_join",
    schedule="50 2 */1 * *",
    start_date=datetime(2024, 8, 27),
    tags=["dm", "src", "userdata"],
    default_args={
        "owner": "wudext",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    make_join()
