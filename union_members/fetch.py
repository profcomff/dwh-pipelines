import logging
from datetime import datetime, timedelta

import pandas as pd
import requests as r
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


@task(task_id="send_telegram_message", retries=3)
def send_telegram_message(chat_id):
    """Скачать данные из ЛК ОПК"""

    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": "Произошла ошибка при скачивании данных из БД ОПК",
        },
    )


@task(task_id="fetch_users", outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_union_members():
    """Скачать данные из ЛК ОПК"""

    with r.Session() as s:
        logging.info(
            "Using user %s to fetch", Variable.get("LK_MSUPROF_ADMIN_USERNAME")
        )

        resp = s.post(
            "https://api-lk.msuprof.com/api/auth/token/login/",
            data={
                "email": str(Variable.get("LK_MSUPROF_ADMIN_USERNAME")),
                "password": str(Variable.get("LK_MSUPROF_ADMIN_PASSWORD")),
            },
        )
        logging.info(resp)
        token = resp.json()["auth_token"]

        resp = s.get(
            "https://api-lk.msuprof.com/api/auth/users/",
            headers={
                "Authorization": f"token {token}",
            },
        )
        logging.info(resp)

    try:
        users_dict = resp.json()
    except Exception as e:
        logging.error("Failed to fetch data from lk.msuprof.com")
        raise e

    for i in users_dict:
        if "card" not in i or i["card"] is None:
            continue
        i["card_id"] = i["card"].get("id")
        i["card_status"] = i["card"].get("status")
        i["card_date"] = i["card"].get("date")
        i["card_number"] = i["card"].get("number")
        i["card_user"] = i["card"].get("user")
        del i["card"]
    data = pd.DataFrame(users_dict)
    logging.info(list(data.columns))
    data.to_sql(
        "union_member",
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
        .replace("?__extra__=%7B%7D", ""),
        schema="STG_UNION_MEMBER",
        if_exists="replace",
        index=False,
    )
    return len(data)


@dag(
    schedule="0 0 */1 * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh", "union_member"],
    default_args={
        "owner": "dyakovri",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": lambda: send_telegram_message(
            int(Variable.get("TG_CHAT_MANAGERS"))
        ),
    },
)
def union_member_download():
    union_members_result = fetch_union_members()


union_member_sync = union_member_download()
