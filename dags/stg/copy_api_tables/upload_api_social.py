from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable

from plugins.api_utils import send_telegram_message, copy_table_to_dwh

# декорированные функции
send_telegram_message = task(
    task_id="send_telegram_message", trigger_rule="one_failed"
)(send_telegram_message)

copy_table_to_dwh = task(
    task_id="copy_table_to_dwh", trigger_rule="one_done", retries=0
)(copy_table_to_dwh)


with DAG(
    dag_id="upload_api_social",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "stg", "social"],
    default_args={
        "owner": "dyakovri",
        "retries": 5,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
    },
):
    tables = (
        "create_group_request",
        "group",
        "telegram_channel",
        "telegram_chat",
        "vk_chat",
        "vk_group",
        "webhook_storage",
    )
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    prev = None
    for table in tables:
        curr = copy_table_to_dwh.override(
            task_id=f"copy-{table}",
            outlets=[Dataset(f"STG_SOCIAL.{table}")],
        )(
            "api_social",
            table,
            "STG_SOCIAL",
            table,
        )
        # Выставляем копирование по порядку
        if prev:
            prev >> curr
        prev = curr
        prev >> tg_task
