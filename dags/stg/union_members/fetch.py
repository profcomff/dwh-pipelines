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
        logging.info("Using user %s to fetch", Variable.get("LK_MSUPROF_ADMIN_USERNAME"))

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

    if users_dict:
        all_keys = set()
        for user in users_dict:
            all_keys.update(user.keys())

        logging.info(f"All fields from API: {sorted(all_keys)}")

    for i in users_dict:
        if "card" not in i:
            continue
        if i["card"] is None:
            del i["card"]
            continue
        i["card_id"] = i["card"].get("id")
        i["card_status"] = i["card"].get("status")
        i["card_date"] = i["card"].get("date")
        i["card_number"] = i["card"].get("number")
        i["card_user"] = i["card"].get("user")
        del i["card"]
    data = pd.DataFrame(users_dict)
    logging.info(f"DataFrame columns: {list(data.columns)}")
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


# ТАСКА ВЫВОДА ПОЛЕЙ ДЛЯ ENDPOINT /api/auth/users/me/
@task(task_id="get_fields", outlets=Dataset("STG_UNION_MEMBER.union_member"))
def get_api_fields():
    """Скачать данные из ЛК ОПК - ТОЛЬКО ЛОГИРОВАНИЕ АДМИНА"""

    with r.Session() as s:
        logging.info("Using user %s to fetch", Variable.get("LK_MSUPROF_ADMIN_USERNAME"))

        resp = s.post(
            "https://api-lk.msuprof.com/api/auth/token/login/",
            data={
                "email": str(Variable.get("LK_MSUPROF_ADMIN_USERNAME")),
                "password": str(Variable.get("LK_MSUPROF_ADMIN_PASSWORD")),
            },
        )
        logging.info(f"Login response status: {resp.status_code}")
        token = resp.json()["auth_token"]

        resp = s.get(
            "https://api-lk.msuprof.com/api/auth/users/me/",
            headers={
                "Authorization": f"token {token}",
            },
        )
        logging.info(f"Me endpoint response status: {resp.status_code}")

    try:
        admin_data = resp.json()
        logging.info("=" * 50)
        logging.info("ADMIN USER DATA FROM /me/ ENDPOINT:")
        logging.info("=" * 50)

        # Логируем все поля
        for key, value in admin_data.items():
            logging.info(f"{key}: {value}")

        keys_list = list(admin_data.keys())

        logging.info("=" * 50)
        logging.info(f"ALL AVAILABLE FIELDS: {sorted(keys_list)}")
        logging.info("=" * 50)
        logging.info(f"TOTAL FIELDS COUNT: {len(keys_list)}")
        logging.info("=" * 50)
        logging.info(f"DATAFRAME SHAPE: {pd.DataFrame(keys_list, columns=['fields']).shape}")
        logging.info("=" * 50)

    except Exception as e:
        logging.error("Failed to fetch data from lk.msuprof.com")
        logging.error(f"Response text: {resp.text}")
        raise e

    logging.info("FUNCTION COMPLETED - CHECK LOGS ABOVE FOR ADMIN DATA")
    return 0


@dag(
    schedule="0 0 */1 * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh", "union_member"],
    default_args={
        "owner": "dyakovri",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": lambda: send_telegram_message(int(Variable.get("TG_CHAT_MANAGERS"))),
    },
)
def union_member_download():
    # union_members_result = fetch_union_members()
    union_members_result = get_api_fields()


union_member_sync = union_member_download()
