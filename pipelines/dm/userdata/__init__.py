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
    outlets=Dataset("DM_USER.union_member_join"),
)
def make_join():
    sql_engine = sa.create_engine(DB_DSN)
    sql_engine.execute(
        """
        INSERT INTO "DM_USER".union_member_join
        select 
        userdata.user_id as userdata_user_id,
        userdata.full_name as full_name,
        um.first_name as first_name,
        um.last_name as last_name,
        um.type_of_learning as type_of_learning,
        um.rzd_status as wtf_value,
        um.academic_level as academic_level,
        um.rzd_number as rzd_number,
        um.card_id as card_id,
        um.card_number as card_number
        from 
        (SELECT 
        STRING_TO_ARRAY(email, ',') as email_list,
        user_id,
        full_name
        FROM "DWH_USER_INFO".info
        ) as userdata
        join "STG_UNION_MEMBER".union_member as um
        on um.email=any(userdata.email_list)
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
