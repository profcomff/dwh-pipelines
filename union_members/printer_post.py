import logging
from datetime import datetime, timedelta
from textwrap import dedent
from urllib.parse import urljoin

import pandas as pd
import requests as r
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


def send_print_post_error_telegram_message():
    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": -633287506,
            "text": f"Ошибка при загрузке данных из БД ОПК в БД принтера",
        }
    )


@task(task_id="post_data", retries=3)
def post_data(url, token):
    con = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")
    query = dedent("""
        SELECT last_name, card_number,faculty
        FROM "STG_UNION_MEMBER"."union_member"
        WHERE strpos(lower(faculty), 'физический факультет'::text) > 0  -- "физический факультет" есть в названии факультета
            AND lower(status) = 'член профсоюза'
            AND LENGTH(last_name) > 0
            AND LENGTH(card_number) > 0;
    """
    )
    data = pd.read_sql_query(query, con).drop_duplicates(subset=["card_number"])

    users = []
    for i, row in data.iterrows():
        user = {
            "username": row["last_name"],
            "union_number": row["card_number"],
        }
        users.append(user)

    resp = r.post(
        urljoin(url, "is_union_member"),
        json={"users": users},
        headers={"Authorization": token},
    )
    logging.info(str(resp.json()))
    if resp.status_code != 200:
        raise Exception(f"Failed to upload {resp.status_code}")
    logging.info("data length: " + str(len(data)))


@dag(
    schedule=[Dataset("STG_UNION_MEMBER.union_member")],
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={
        "owner": "SergeyZamyatin1",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    on_failure_callback=send_print_post_error_telegram_message,
)
def update_printer_user_list():
    post_data("https://api.profcomff.com/print/", str(Variable.get("TOKEN_ROBOT_PRINTER")))


update_printer_user_list()
