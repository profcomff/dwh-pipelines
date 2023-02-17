import logging
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection

from timetable.utils.parse_timetable import run

DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")


@task(inlets=Dataset("STG_TIMETABLE.raw_html"), outlets=Dataset("ODS_TIMETABLE.stage"))
def parse_all():
    sql_engine = sa.create_engine(DB_URI, pool_pre_ping=True)
    raw = sql_engine.execute('SELECT url, raw_html FROM "STG_TIMETABLE".raw_html;')
    results = pd.DataFrame()
    for (url, html) in raw:
        try:
            results = pd.concat([results, pd.DataFrame(run(html))])
        except Exception:
            logging.warning(f"Парсинг завершился ошибкой: {url}")
            raise
    results.to_sql('stage', sql_engine, schema='ODS_TIMETABLE', index=False, if_exists='replace')
    return len(results)


@task(
    inlets=[Dataset("STG_TIMETABLE.raw_html"), Dataset("ODS_TIMETABLE.stage"), Dataset("ODS_TIMETABLE.hist")],
    outlets=[Dataset("ODS_TIMETABLE.hist"), Dataset("ODS_TIMETABLE.diff")],
)
def save_hist():
    pass


@dag(
    schedule=[Dataset("STG_TIMETABLE.raw_html")],
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags= ["dwh"],
    default_args={
        "owner": "dwh",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def timetable_parse():
    parse_all() >> save_hist()


timetable_parse()
