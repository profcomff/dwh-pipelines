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
token = Variable.get("TOKEN_ROBOT_TIMETABLE")
headers = {"Authorization": f"{token}"}
environment = Variable.get("_ENVIRONMENT")


@task(task_id='restart', outlets=Dataset("STG_RASPHYSMSU.old"))
def restart():
    start_deleting = datetime.datetime.now()
    start_deleting = start_deleting.strftime("%Y-%m-%d")
    end_deleting = Variable.get("SEMESTER_END")
    end_deleting = datetime.datetime.strptime(end_deleting, '%m/%d/%Y')
    end_deleting = datetime.datetime.strftime(end_deleting, "%Y-%m-%d")
    if environment == "test":
        url = f'https://api.test.profcomff.com/timetable/event/bulk?start={start_deleting}&end={end_deleting}'
        r = requests.delete(url, headers=headers)
        logging.info(r)
    if environment == "prod":
        url = f'https://api.profcomff.com/timetable/event/bulk?start={start_deleting}&end={end_deleting}'
        r = requests.delete(url, headers=headers)
        logging.info(r)

    engine = sa.create_engine(DB_URI)
    engine.execute("""
    delete from "STG_RASPHYSMSU"."old";
    delete from "STG_RASPHYSMSU"."new";
    delete from "STG_RASPHYSMSU".diff;
    """)


@dag(
    schedule=None,
    start_date=datetime.datetime(2023, 8, 1, 2, 0, 0),
    max_active_runs=1,
    catchup=False,
    tags= ["dwh", "timetable"],
    default_args={
        "owner": "zamyatinsv",
        "retries": 0,
        "retry_delay": datetime.timedelta(minutes=5)
    }
)
def restart_timetable():
    restart()


sync = restart_timetable()
