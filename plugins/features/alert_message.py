import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG

from airflow.models import Variable
import requests as r

ENVIRONMENT = Variable.get("_ENVIRONMENT")


def alert_message(context, chat_id):
    token = str(Variable.get("TGBOT_TOKEN"))

    # Параметры сообщения
    dag_id = context['dag'].dag_id
    owner = context['dag'].owner
    dag_url = f"https://localhost:8080/dags/{dag_id}/grid"

    # Дебильные эмодзи чтобы раздражать людей
    message = f"🚨 *DAG Failed* 🚨 🗣🗣🗣\n\n*DAG ID*: {dag_id}\n*Owner*: {owner}\n*Dag URL*: {dag_url}" 
    tg_api_url = f"https://api.telegram.org/bot{token}/sendMessage"

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
    req.raise_for_status()
