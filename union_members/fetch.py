import logging
from datetime import datetime, timedelta

import pandas as pd
import requests as r
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


@task(task_id='send_telegram_message', retries=3)
def send_telegram_message(chat_id, data_length):
    """Скачать данные из ЛК ОПК"""

    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={
            "chat_id": chat_id,
            "text": "Получено {} строк из базы ОПК".format(data_length),
        }
    )


@task(task_id='fetch_users', outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_union_members():
    """Скачать данные из ЛК ОПК"""

    with r.Session() as s:
        logging.info("Using user %s to fetch", Variable.get("LK_MSUPROF_ADMIN_USERNAME"))

        s.headers = {
            "referer": "https://lk.msuprof.com/adminlogin/?next=/admin",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',  # noqa
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'  # noqa
        }

        resp = s.get(
            "https://lk.msuprof.com/adminlogin/?next=/admin",
        )
        logging.info(resp)

        resp=s.post(
            "https://lk.msuprof.com/adminlogin/?next=/admin",
            data={
                "csrfmiddlewaretoken": s.cookies['csrftoken'],
                "username": str(Variable.get("LK_MSUPROF_ADMIN_USERNAME")),
                "password": str(Variable.get("LK_MSUPROF_ADMIN_PASSWORD")),
                "next": "/admin",
            },
        )
        logging.info(resp)

        resp = s.post(
            "https://lk.msuprof.com/get-table/",
            data={
                "current-role": "Администратор",
                "f_role": "",
                "f_status": "",
                "page-status": "user",
            },
        )
        logging.info(resp)

    try:
        users_dict = resp.json()["data"]
    except Exception as e:
        logging.error("Failed to fetch data from lk.msuprof.com")
        raise e

    data = pd.DataFrame(users_dict)
    data.to_sql(
        'union_member',
        Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://"),
        schema='STG_UNION_MEMBER',
        if_exists='replace',
        index=False,
    )
    return len(data)


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
def union_member_download():
    union_members_result = fetch_union_members()
    send_telegram_message(-633287506, union_members_result)


union_member_sync = union_member_download()
