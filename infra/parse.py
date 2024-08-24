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

def parse_data(data):
    event_text = []
    group_text = []
    time_interval_text = []
    final_massive = []
    for item in data:
        print(item[1])
        counter =0
        soup = BeautifulSoup(item[1],'html.parser')
        print(f'soup:{soup}') #всегда 6 пар в день
        for i in soup.find_all('td',class_=['tditem1','tdsmall']):
            counter +=1
            o = i.get_text()
            if (counter<=6):
                o = o + " Понедельник"
            elif (6<counter<=12):
                o = o + " Вторник"
            elif (12<counter<=18):
                o = o + " Среда"
            elif (18<counter<=24):
                o = o + " Четверг"
            elif (24<counter<=30):
                o = o + " Пятница"
            elif (30<counter<=36):
                o = o +" Суббота"
            event_text.append(o)
        for j in soup.find_all('td', class_='tdtime'):
            time_interval_text.append(j.get_text())
        for u in soup.find_all('tr', class_='tdheader'):
            sample = re.compile(r"\d{3}")
            res_middle = u.get_text()
            for v in range(36):
                group_text.append((sample.search(res_middle)).group(0))
            print(f'gr:{group_text}')
        for h in range(len(event_text)):
            final_massive.append([event_text[h],time_interval_text[h],group_text[h]])
            final_massive[h][0].replace('\xa0','')
            print(f'fn:{final_massive}')

        return final_massive

@task(task_id='get_from_database_data', inlets=Dataset("STG_RASPHYSMSU.raw_html"), outlets =Dataset("ODS_TIMETABLE.ods_timetable_act"))
def get_from_database_data():
    data = []
    DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")
    sql_engine = sa.create_engine(DB_URI)
    with sql_engine.connect() as conn:
        data = conn.execute(sa.text(f'''SELECT * FROM "STG_RASPHYSMSU".raw_html''')).fetchall()
        print(f"Data: {data}")
        logging.info("starting parsing")
        parced = parse_data(data)
        print(f"Parced: {parced}")
        data = pd.DataFrame(parse_data(data))
        print(f"Df: {data}")
        data.to_sql(
            "ods_timetable_act",
            con=sql_engine,
            schema="ODS_TIMETABLE",
            if_exists="replace",
            index=False,
        )
        conn.execute(sa.text(f''' delete from "ODS_TIMETABLE".ods_timetable_act where length("0") < 20 '''))
        return Dataset("ODS_TIMETABLE.ods_timetable_act")
@dag(
    schedule='0 */1 * * *',
    start_date=datetime(2024, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "SofyaFin", "retries": 3, "retry_delay": timedelta(minutes=5)},
)
def parse_Rasphysmsu():
    get_from_database_data()


parse_Rasphysmsu()
