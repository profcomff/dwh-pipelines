import logging
from datetime import datetime, timedelta
import sqlalchemy as sa
import requests as r
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


@task(task_id='send_telegram_message', retries=3)
def send_telegram_message(chat_id):
    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={
            "chat_id": chat_id,
            "text": "DWH база данных устарела!",
        }
    )


@task(task_id='fetch_db', outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_dwh_db():
    schemas = ['achievement', 'auth', 'marketing', 'printer', 'redirector', 'service', 'social', 'timetable', 'userdata']
    api_con = Connection.get_connection_from_secrets('api').get_uri().replace("postgres://", "postgresql://")
    api_sql_engine = sa.create_engine(api_con)

    dwh_con = Connection.get_connection_from_secrets('dwh').get_uri().replace("postgres://", "postgresql://")
    dwh_sql_engine = sa.create_engine(dwh_con)

    for schema in schemas:
        api_schema = ('api_' + schema).upper()
        dwh_schema = ('DWH_' + schema).upper()

        api_tables = api_sql_engine.execute(f'''
                SELECT * FROM information_schema.tables
                WHERE table_schema='{api_schema}'
            ''').fetchall()

        dwh_tables = dwh_sql_engine.execute(f'''
                SELECT * FROM information_schema.tables
                WHERE table_schema='{dwh_schema}'
            ''').fetchall()

        logging.info(msg=(api_tables, dwh_tables))



@dag(
    schedule='0 0 */1 * *',
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags= ["dwh"],
    default_args={
        "owner": "dwh",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def integrity_check():
    result = fetch_dwh_db()
    send_telegram_message(-633287506)


dwh_integrity_check = integrity_check()
