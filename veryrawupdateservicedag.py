import logging
from datetime import datetime, timedelta
import re
import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from bs4 import BeautifulSoup


@task(task_id='download_pages_to_db', outlets=Dataset("STG_RASPHYSMSU.raw_html"))
def get_from_database_data():
    DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")
    sql_engine = sa.create_engine(DB_URI)
    with sql_engine.connect() as conn:
        event_text = []
        group_text = []
        time_interval_text = []
        data = conn.execute(sa.text(f'''SELECT * FROM "STG_RASPHYSMSU".raw_html''')).fetchall()
        logging.info("starting parsing")
        def _parse_data(data):
            counter_1 = 0
            counter_2 = 0
            counter_3 = 0
            soup = BeautifulSoup(data)
            for i in soup.find_all('tr',class_=['tditem1','tdsmall']):
                event_text[counter_1] = i.get_text()
                counter_1+=1
            for j in soup.find_all('tr', class_='tdtime'):
                time_interval_text[counter_2] = j.get_text()
                counter_2+=1
            for u in soup.find_all('b'):
                sample = re.compile(r"\d{3}")
                res_middle = u.get_text()
                group_text[counter_3] = sample.search(res_middle)
                counter_3+=1
    conn.execute(sa.text(f'''delete from "ODS_TIMETABLE".ods_timetable_act
    CREATE TABLE IF NOT EXISTS "ODS_TIMETABLE".ods_timetable_act (url varchar(256) NULL, group_text text NULL, time_interval_text text NULL, event_text text NULL);'''))
    conn.commit()
    for i in range(len(data)):
        conn.execute(sa.text(f'''alter table "ODS_TIMETABLE".ods_timetable_act insert into (group_text,time_interval_act,time_interval_text) values ({group_text[i],time_interval_text[i],event_text[i]} '''))
        conn.commit()
@dag(
    schedule='0 */1 * * *',
    start_date=datetime(2024, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "dwh", "retries": 3, "retry_delay": timedelta(minutes=5)},
)
def start():
    get_from_database_data()

start()




