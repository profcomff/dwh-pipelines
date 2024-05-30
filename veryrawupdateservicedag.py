import logging
from datetime import datetime, timedelta
import re
import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
import BeautifulSoup from bs4


@task(task_id='download_pages_to_db', outlets=Dataset("STG_RASPHYSMSU".raw_html)
def get_from_database_data():
    DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")
    sql_engine = sa.create_engine(DB_URI)
    with dwh_sql_engine.connect() as conn:
        event_text = []
        group_text = []
        time_interval_text = []
        data = conn.execute(sa.text(f'''SELECT * FROM "STG_RASPHYSMSU".raw_html''')).fetchall()
        logging.info("starting parsing")
        def _parse_data(data):
            k1 = 0
            k2 = 0
            k3 = 0
            soup = BeautifulSoup(data)
            for i in soup.find_all('tr',class_=['tditem1','tdsmall']):
            event_text[k1] = i.get_text()
            k1+=1
            for j in soup.find_all('tr', class_='tdtime'):
                time_interval_text[k2] = j.get_text()
                k2+=1
            for u in soup.find_all('b'):
                sample = re.compile(r"\d{3}")
                res_middle = u.get_text()
                group_text[k3] = sample.search(res_middle)
                k3+=1
    conn.execute(sa.text(f'''delete from ods_timetable_act'''))
    conn.execute(sa.text(f''' CREATE TABLE IF NOT EXISTS ods_timetable_act (url varchar(256) NULL, group_text text NULL, time_interval_text text NULL, event_text text NULL);'''))
    for i in range(len(data)):
        conn.execute(sa.text(f'''alter table ods_timetable_act insert into (group_text,time_interval_act,time_interval_text) values ({group_text[i],time_interval_text[i],event_text[i]} '''))

@dag(
    def start():
        get_from_database_data()
    schedule='0 */1 * * *',
    start_date=datetime(2024, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "dwh", "retries": 3, "retry_delay": timedelta(minutes=5)},
    start()
    
)


