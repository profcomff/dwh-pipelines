import logging

import sqlalchemy as sa
import datetime
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable

DB_URI = (
    Connection.get_connection_from_secrets('postgres_dwh')
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)
token = Variable.get("TOKEN_ROBOT_TIMETABLE", "")
headers = {"Authorization": f"{token}"}
environment = Variable.get("_ENVIRONMENT", "")


@task(task_id='restart', inlets=Dataset("STG_RASPHYSMSU.new"))
def restart():
    engine = sa.create_engine(DB_URI)
    events = engine.execute("""
    select * from "STG_RASPHYSMSU"."new"
    """)
    res = []
    for event in events:
        res.append(
            {
                "name": event["subject"],
                "room_id": event["room"],
                "group_id": event["room"],
                "lecturer_id": event["room"],
                "start_ts": event["start"],
                "end_ts": event["end"],
            }
        )
        logging.info(event)
    if environment == "test":
        url = f'https://api.test.profcomff.com/timetable/event/bulk'
        r = requests.post(url, headers=headers, json=res)
        logging.info(r)
    if environment == "prod":
        url = f'https://api.profcomff.com/timetable/event/bulk'
        r = requests.post(url, headers=headers, json=res)
        logging.info(r)


@dag(
    schedule=None,
    start_date=datetime.datetime(2023, 8, 1, 2, 0, 0),
    max_active_runs=1,
    catchup=False,
    tags= ["dwh", "timetable"],
    default_args={
        "owner": "mixx3",
        "retries": 0,
        "retry_delay": datetime.timedelta(minutes=5)
    }
)
def bulk_insert_timetable():
    restart()


sync = bulk_insert_timetable()
