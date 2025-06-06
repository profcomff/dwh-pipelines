from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable

from plugins.api_utils import copy_table_to_dwh, send_telegram_message
from plugins.features import alert_message


# декорированные функции
send_telegram_message = task(task_id="send_telegram_message", trigger_rule="one_failed")(send_telegram_message)

copy_table_to_dwh = task(task_id="copy_table_to_dwh", trigger_rule="one_done", retries=0)(copy_table_to_dwh)


with DAG(
    dag_id="upload_api_rating",
    start_date=datetime(2024, 12, 14),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "stg", "rating"],
    default_args={
        "owner": "dyakovri",
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
    },
):
    tables = (
        "lecturer",
        "comment",
        "lecturer_user_comment",
    )
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    prev = None
    for table in tables:
        curr = copy_table_to_dwh.override(
            task_id=f"copy-{table}",
            outlets=[Dataset(f"STG_RATING.{table}")],
        )(
            "api_rating",
            table,
            "STG_RATING",
            table,
        )
        # Выставляем копирование по порядку
        if prev:
            prev >> curr
        prev = curr
        prev >> tg_task
