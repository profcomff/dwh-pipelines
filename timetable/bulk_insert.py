import logging
import time
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


@task(task_id='bulk_insert', inlets=Dataset("STG_RASPHYSMSU.new_with_dates"))
def bulk_insert():
    engine = sa.create_engine(DB_URI)
    batch_delta = 100
    total_size = 20000  # примерно
    batches = [batch_delta for d in range(total_size//batch_delta + 1)]
    offset = 0
    for batch in batches:
        events = engine.execute(f"""
        select * from "STG_RASPHYSMSU"."new_with_dates"
        limit {batch}
        offset {offset}
        """)
        res = []
        offset += batch
        for event in events:
            res.append(
                {
                    "name": event["subject"],
                    "room_id": event["place"],
                    "group_id": event["group"],
                    "lecturer_id": event["teacher"],
                    "start_ts": event["start"],
                    "end_ts": event["end"],
                }
            )
            logging.info(event)
        inserted = False  # for retries
        retries_max_cnt = 10
        retries_cnt = 0
        if environment == "test":
            url = f'https://api.test.profcomff.com/timetable/event/bulk'
            while not inserted and retries_cnt < retries_max_cnt:
                r = requests.post(url, headers=headers, json=res)
                logging.info(f"{r.status_code=}")
                if r.status_code == 200:
                    inserted = True
                else:
                    retries_cnt += 1
                    logging.info(f"tryna insert {batch=}, {retries_cnt=}")
                    time.sleep(1)
        if environment == "prod":
            url = f'https://api.profcomff.com/timetable/event/bulk'
            while not inserted and retries_cnt < retries_max_cnt:
                r = requests.post(url, headers=headers, json=res)
                logging.info(f"{r.status_code=}")
                if r.status_code == 200:
                    inserted = True
                else:
                    retries_cnt += 1
                    logging.info(f"tryna insert {batch=}, {retries_cnt=}")
                    time.sleep(1)


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
    bulk_insert()


sync = bulk_insert_timetable()
