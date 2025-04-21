from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable

from plugins.api_utils import send_telegram_message as send_msg, copy_table_to_dwh as copy_tbl

@task(task_id="send_telegram_message", trigger_rule="one_failed")
def send_telegram_message(chat_id, **context):
    return send_msg(chat_id, **context)

@task(task_id="copy_table_to_dwh", trigger_rule="one_done", retries=0)
def copy_table_to_dwh(from_schema, from_table, to_schema, to_table):
    return copy_tbl(from_schema, from_table, to_schema, to_table)

with DAG(
    dag_id="upload_api_rental",
    start_date=datetime(2024, 4, 18),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "stg", "rental"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    tables = (
        "event",
        "item",
        "item_type",
        "rental_sessions",
        "strike"
    )
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    prev = None
    for table in tables:
        curr = copy_table_to_dwh.override(
            task_id=f"copy-{table}",
            outlets=[Dataset(f"STG_RENTAL.{table}")],
        )(
            "api_rental",
            table,
            "STG_RENTAL",
            table,
        )
        # Выставляем копирование по порядку
        if prev:
            prev >> curr
        prev = curr
        prev >> tg_task
