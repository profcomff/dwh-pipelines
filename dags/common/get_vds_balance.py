import logging
from datetime import datetime, timedelta
from functools import partial

import requests as r
import urllib3
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from plugins.features import alert_message


ENVIRONMENT = Variable.get("_ENVIRONMENT")
url = "https://my.vds.sh/manager/billmgr"


@task(task_id="send_telegram_message", retries=3)
def send_telegram_message_or_print(chat_id, balance):
    """Скачать данные из ЛК ОПК"""

    if balance > 700:
        logging.info(f"Баланс {balance} рублей")
    else:
        token = str(Variable.get("TGBOT_TOKEN"))
        r.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": f"Баланс vds.sh составляет {balance} рублей. Время пополнить счет, @Mark_Shidran!",
            },
        )


@task(task_id="fetch_users", retries=3)
def get_balance():
    """Скачать данные из ЛК VDS.sh"""
    urllib3.disable_warnings()

    with r.Session() as session:
        username = str(Variable.get("LK_VDSSH_ADMIN_USERNAME"))
        password = str(Variable.get("LK_VDSSH_ADMIN_PASSWORD"))

        session.get(url, verify=False)

        login_data = {'func': 'auth', 'username': username, 'password': password}

        auth_response = session.post(url, data=login_data, verify=False)
        logging.info(f"Auth response status: {auth_response.status_code}")

        try:
            auth_json = auth_response.json()
            if auth_json['doc']['$func'] == 'logon':
                logging.error("Авторизация не удалась, все еще на странице входа")
                return None
        except Exception as e:
            logging.info(f"Не удалось распарсить JSON ответа авторизации: {e}")

        balance_params = {'func': 'desktop', 'startform': 'vds', 'out': 'xjson'}

        balance_response = session.get(url, params=balance_params, verify=False)
        logging.info(f"Balance response status: {balance_response.status_code}")
        balance_data = balance_response.json()
        balance = float(balance_data.get('doc', {}).get('user', {}).get('$balance', str()))

        if balance is None:
            logging.info("Баланс не был получен")
            raise ValueError

        return balance


with DAG(
    dag_id="check_vds_balance",
    schedule="0 */12 * * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["infra"],
    default_args={
        "owner": "dyakovri",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
    },
) as dag:
    balance = get_balance()
    send_telegram_message_or_print(int(Variable.get("TG_CHAT_MANAGERS")), balance)
