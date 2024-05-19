import logging
from datetime import datetime, timedelta
import re
import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")


@task(task_id='download_pages_to_db', outlets=Dataset("STG_".raw_html))
def create_data():
    data = []
    i = 0
    while (response = r.get(url, headers=HEADERS)): #типо пока делается делай, пока данные читаются, читай
        url = f'http://ras.phys.msu.ru/table/{data[0]}/{data[1]}/{i}.htm'
        i +=1
        logging.info("Page %s fetched with status %d", url, response.status_code)
        data.append(
            {
                'url': url,
                'raw_html': response,
            }
        )
        logging.info("starting parsing url and raw_html")
        def _parse_data(data):
            parsed_raw = {"event_text": None, "time_interval_text": None, "group_text": None}
            data = _preprocessing(data)

            # '... <nobr>12:00</nobr> 110
            res = re.match(r"/\d+/g")
            result = re.match(r"([А-Яа-яёЁa-zA-Z +,/.\-\d]+)+<nobr>([А-Яа-яёЁa-zA-Z +,/.\-\d]+)</nobr>"+r"/\d+/g")
            if not (result is None):
                if data == result[0]:
                    parsed_raw["event_text"] = result[1]
                    parsed_raw["time_interval_text"] = result[2]
                    parsed_raw["group_text"] = result[3]
                    return parsed_raw

    sql_engine = sa.create_engine(DB_URI)

    conn.commit()
    sql_engine.execute(
        """
    CREATE TABLE IF NOT EXISTS "STG_TIMETABLE".raw_html (url varchar(256) NULL, group_text text NULL, time_interval_text text NULL, event_text text NULL);
    CREATE TABLE IF NOT EXISTS "STG_TIMETABLE".raw_html_old (url varchar(256) NULL, group_text text NULL, time_interval_text text NULL, event_text text NULL);
    """
    )
    sql_engine.execute(
        """
    delete from "STG_TIMETABLE".raw_html_old;
    """
    )
    logging.info("raw_html_old is empty")
    sql_engine.execute(
        """
    insert into "STG_TIMETABLE".raw_html_old ("url", "event_text", "time_interval_text","group_text") select "url", "event_text","time_interval_text","group_text" from "STG_TIMETABLE".raw_html
    """
    )
    logging.info("raw_html_old is full")
    sql_engine.execute(
        """
    delete from "STG_TIMETABLE".raw_html;
    """
    )
    logging.info("raw_html is empty")
    data.to_sql('event_text'+'time_interval_text'+'group_text', sql_engine, schema='STG_TIMETABLE', if_exists='append', index=False)
    logging.info("raw_html is full")
    return Dataset("STG_TIMETABLE.raw_html")



@dag(
    schedule='0 */1 * * *',
    start_date=datetime(2024, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "dwh", "retries": 3, "retry_delay": timedelta(minutes=5)},
)


