import logging
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from plugins.api_utils import copy_rating_to_rating_api, send_telegram_message
from plugins.features import alert_message


copy_rating_to_rating_api = task(task_id="copy_rating_to_rating_api", trigger_rule="one_done", retries=0)(
    copy_rating_to_rating_api
)
send_telegram_message = task(task_id="send_telegram_message", trigger_rule="one_failed")(send_telegram_message)


with DAG(
    dag_id="copy_lecturer_rating",
    start_date=datetime(2025, 8, 23),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "api", "rating", "copy"],
    default_args={
        "owner": "VladislavVoskoboinik",
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
    },
):
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    copy = copy_rating_to_rating_api()

    copy >> tg_task
