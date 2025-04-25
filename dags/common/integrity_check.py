import logging
from datetime import datetime, timedelta

import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from plugins.api_utils import fetch_dwh_db, send_telegram_message


send_telegram_message = task(task_id="send_telegram_message", retries=3)(send_telegram_message)


with DAG(
    dag_id="dwh_integrity_check",
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "infra"],
    default_args={"owner": "roslavtsevsv"},
) as dag:
    result = fetch_dwh_db()
    send_telegram_message(int(Variable.get("TG_CHAT_DWH")), result)
    send_telegram_message(int(Variable.get("TG_CHAT_MANAGERS")), result)
