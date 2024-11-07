import logging
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
import requests

API_LINK = Variable.get("API_LINK")
DWH_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)
TOKEN_ROBOT_TIMETABLE = Variable.get("TOKEN_ROBOT_TIMETABLE")


@task(task_id="send_lecturers", retries=3)
def send_lecturers():
    dwh_sql_engine = create_engine(DWH_DB_DSN)
    with dwh_sql_engine.connect() as dwh_conn:
        lecturers = dwh_conn.execute(
            sa.text(
                """SELECT lecturer_first_name, lecturer_middle_name, lecturer_last_name, lecturer_avatar_id, lecturer_api_id FROM "DM_TIMETABLE".dim_lecturer_act"""
            )
        ).fetchall()

    for lectuter in lecturers:
        avatar_link = dwh_conn.execute(f'''SELECT link FROM "STG_TIMETABLE WHERE id={lectuter[3]}"''')
        body = {
            "first_name": lectuter[0],
            "middle_name": lectuter[1],
            "last_name": lectuter[2],
            "avatar_link": avatar_link,
            "timetable_id": lectuter[4]
        }
        requests.post(
            f"{API_LINK}/timetable/lecturer/",
            headers={"Authorization": TOKEN_ROBOT_TIMETABLE},
            body=body,
        )


with DAG(
    dag_id="send_lecturers_timetable",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "timetable"],
    default_args={"owner": "roslavtsevsv"},
) as dag:
    send_lecturers()
