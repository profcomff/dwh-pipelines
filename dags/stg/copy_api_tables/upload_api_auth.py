from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable

from plugins.api_utils import copy_table_to_dwh, send_telegram_message


# декорированные функции
send_telegram_message = task(task_id="send_telegram_message", trigger_rule="one_failed")(send_telegram_message)

copy_table_to_dwh = task(task_id="copy_table_to_dwh", trigger_rule="one_done", retries=0)(copy_table_to_dwh)


with DAG(
    dag_id="upload_api_auth",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "stg", "auth"],
    default_args={"owner": "dyakovri"},
):
    tables = (
        "auth_method",
        "dynamic_option",
        "group",
        "group_scope",
        "scope",
        "user",
        "user_group",
        "user_message_delay",
        "user_session",
        "user_session_scope",
    )
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    prev = None
    for table in tables:
        curr = copy_table_to_dwh.override(
            task_id=f"copy-{table}",
            outlets=[Dataset(f"STG_AUTH.{table}")],
        )(
            "api_auth",
            table,
            "STG_AUTH",
            table,
        )
        # Выставляем копирование по порядку
        if prev:
            prev >> curr
        prev = curr
        prev >> tg_task
