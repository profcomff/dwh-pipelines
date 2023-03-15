import logging
import requests as r
import pandas as pd
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable, Connection
from datetime import datetime, timedelta

class FailedUpload(Exception):

    def __init__(self, *args):
        super().__init__(*args)


@task(task_id='send_print_post_telegram_message', retries=3)
def send_print_post_telegram_message(chat_id):
    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={
            "chat_id": chat_id,
            "text": f"Данные из БД ОПК загружены в продовую БД принтера",
        }
    )


@task(task_id='send_print_post_error_telegram_message', retries=3)
def send_print_post_error_telegram_message(chat_id):
    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={
            "chat_id": chat_id,
            "text": f"Ошибка при загрузке данных из БД ОПК в продовую БД принтера",
        }
    )

@task(task_id='post_data', retries=3)
def post_data(env):
    table = '"STG_UNION_MEMBER".union_member'
    if env == "prod":
        url = "https://api.profcomff.com/print/"
        token = str(Variable.get("TOKEN_ROBOT_PRINTER_PROD"))
    else:
        url = "https://api.test.profcomff.com/print/"
        token = str(Variable.get("TOKEN_ROBOT_PRINTER_TEST"))

    headers = {"Authorization": token}
    con = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")

    users = []

    query = f"""
    SELECT last_name, profcom_id FROM {table}
    WHERE(faculty='Физический факультет') AND(status='Члены Профсоюза') AND(length(last_name)>0) AND(length(profcom_id)>0)
    """

    data = pd.read_sql_query(query, con)
    data = data.drop_duplicates(subset=['profcom_id'])

    for i, row in data.iterrows():
        surname = row['last_name']
        number = row['profcom_id']

        if number:
            user = {
                "username": surname,
                "union_number": number
            }
            users.append(user)

    users_new = {
        "users": users
    }
    resp = r.post(f"{url}is_union_member", json=users_new, headers=headers)
    logging.info(str(resp.json()))
    if resp.status_code != 200:
        raise FailedUpload(f"Failed to upload {resp.status_code}")
    logging.info("data length: " + str(len(data)))


@dag(
    schedule=[Dataset("STG_UNION_MEMBER.union_member")],
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={
        "owner": "SergeyZamyatin1",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def update_printer_user_list():
    try:
        post_data("test")
        post_data("prod")
    except FailedUpload:
        send_print_post_error_telegram_message(-633287506)
    else:
        send_print_post_telegram_message(-633287506)


update_printer_user_list()
