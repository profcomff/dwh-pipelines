import logging

import requests as r
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator


ENVIRONMENT = Variable.get("_ENVIRONMENT")
TOKEN = str(Variable.get("TGBOT_TOKEN"))


def alert_message(context, chat_id: int | str):
    # Параметры сообщения
    dag_id = context["dag"].dag_id
    owner = context["dag"].owner
    dag_url = f"https://airflow.{'test' if not ENVIRONMENT == 'prod' else ''}.profcomff.com/dags/{dag_id}/grid"
    dag_url = dag_url.replace("=", "\\=").replace("-", "\\-").replace("+", "\\+").replace(".", "\\.").replace("_", "\\_")

    # Дебильные эмодзи чтобы раздражать людей
    message = f"*DAG Failed*\n\n*DAG ID*: {dag_id}\n*Owner*: {owner}\n*Dag URL*: {dag_url}"
    tg_api_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    msg = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "MarkdownV2",
    }
    logging.debug(msg)
    # Отправлка сообщения через api телеграма
    try:
        req = r.post(url=tg_api_url, json=msg, timeout=10)
        req.raise_for_status()
    except Exception as e:
        logging.error(f"Telegram API error: {str(e)}")

    logging.info("Bot send message status %d (%s)", req.status_code, req.text)
    req.raise_for_status()
