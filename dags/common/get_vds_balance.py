import logging
from datetime import datetime, timedelta
from functools import partial

import requests as r
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from plugins.features import alert_message

ENVIRONMENT = Variable.get("_ENVIRONMENT")


def send_message_on_failure(context):
    token = str(Variable.get("TGBOT_TOKEN"))
    chat_id = int(Variable.get("TG_CHAT_DWH"))
    # Параметры сообщения
    dag_id = context["dag"].dag_id
    owner = context["dag"].owner

    message = f"🚨 DAG Failed 🚨\n\nDAG ID {dag_url}: {dag_id}\nOwner: {owner}"
    tg_api_url = f"https://api.telegram.org/bot{token}/sendMessage"

    if ENVIRONMENT == "prod":
        dag_url = f"https://airflow.profcomff.com/dags/{dag_id}/grid"
    else:
        dag_url = f"https://airflow.test.profcomff.com/dags/{dag_id}/grid"

    msg = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "MarkdownV2",
    }
    logging.info(msg)
    # Отправлка сообщения через api телеграма
    try:
        req = r.post(tg_api_url, json=msg, timeout=10)
        req.raise_for_status()
    except Exception as e:
        logging.error(f"Telegram API error: {str(e)}")

    logging.info("Bot send message status %d (%s)", req.status_code, req.text)


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
                "text": f"Баланс vds.sh составляет {balance} рублей. Время пополнить счет, @MArzangulyan!",
            },
        )


@task(task_id="fetch_users", retries=3)
def get_balance():
    """Скачать данные из ЛК ОПК"""

    with r.Session() as s:
        username = str(Variable.get("LK_VDSSH_ADMIN_USERNAME"))
        password = str(Variable.get("LK_VDSSH_ADMIN_PASSWORD"))
        resp = s.get(
            f"https://my.vds.sh/manager?out=sjson&func=auth&username={username}&password={password}"
        )
        auth_id = resp.json()["doc"]["auth"]["$id"]

        resp = s.get(
            f"https://my.vds.sh/manager?out=sjson&func=dashboard.info&auth={auth_id}"
        )
        balance = resp.json()["doc"]["elem"][0]["balance"][0]["$"]
        balance = float(balance.split()[0])

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
        "on_failure_callback": partial(
            send_telegram_message, chat_id=int(Variable.get("TG_CHAT_DWH"))
        ),
    },
) as dag:
    balance = get_balance()
    send_telegram_message_or_print(int(Variable.get("TG_CHAT_MANAGERS")), balance)
