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


environment = Variable.get("_ENVIRONMENT")
API_LINK = f"https://api{'.test' if  environment in {'test', 'development'} else ''}.profcomff.com"
DWH_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)
TOKEN_RATING_TEST = Variable.get("TOKEN_RATING_TEST")


@task(task_id="send_lecturers", retries=3)
def send_lecturers():
    headers = {
            # 'accept': 'application/json',
            'Authorization': f"{TOKEN_RATING_TEST}",
            # 'Content-Type': 'application/json',
        }
    res = requests.get(
        f"{API_LINK}/rating/lecturer/"
    )
    print(res.status_code)
    dwh_sql_engine = create_engine(DWH_DB_DSN)
    with dwh_sql_engine.connect() as dwh_conn:
        lecturers = dwh_conn.execute(
            sa.text("""
                SELECT 
                    id,
                    first_name,
                    middle_name,
                    last_name,
                    avatar_id as avatar_link
                FROM "STG_TIMETABLE".lecturer;
            """)
        ).fetchall()
        logging.info(lecturers[0])

    for lecturer in lecturers:
        body = {
            "first_name": lecturer[1],
            "middle_name": lecturer[2],
            "last_name": lecturer[3],
            "avatar_link": lecturer[4],
            "timetable_id": lecturer[0]
        }
        res = requests.post(
            f"{API_LINK}/rating/lecturer/",
            headers=headers,
            json=body,
        )
        if res.status_code != 200:
            logging.info(res.status_code)
            logging.info(TOKEN_RATING_TEST[:15] + "...." + TOKEN_RATING_TEST[-15:])
            logging.info(res.text)


with DAG(
    dag_id="send_lecturers_timetable",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "timetable"],
    default_args={"owner": "roslavtsevsv"},
) as dag:
    send_lecturers()
