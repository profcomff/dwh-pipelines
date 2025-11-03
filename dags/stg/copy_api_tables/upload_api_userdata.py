from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable

from plugins.api_utils import copy_table_to_dwh, send_telegram_message


# декорированные функции
send_telegram_message = task(task_id="send_telegram_message", trigger_rule="one_failed")(send_telegram_message)

copy_table_to_dwh = task(task_id="copy_table_to_dwh", trigger_rule="one_done", retries=0)(copy_table_to_dwh)


with DAG(
    dag_id="upload_api_userdata",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["dwh", "stg", "userdata"],
    default_args={"owner": "dyakovri"},
):
    tables = (
        "category",
        "info",
        "param",
        "source",
    )
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    prev = None
    for table in tables:
        curr = copy_table_to_dwh.override(
            task_id=f"copy-{table}",
            outlets=[Dataset(f"STG_USERDATA.{table}")],
        )(
            "api_userdata",
            table,
            "STG_USERDATA",
            table,
        )
        # Выставляем копирование по порядку
        if prev:
            prev >> curr
        prev = curr
        prev >> tg_task


with DAG(
    dag_id="upload_api_userdata_encrypted",
    start_date=datetime(2025, 4, 24),
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["dwh", "stg", "userdata", "encryption"],
    default_args={
        "owner": "sotov",
        "retries": 5,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
    },
):
    # [ struct (table_name: str, encrypt_columns: [str], owner_column: [str]) ]
    tables = (("info", ["value"], "owner_id"),)
    tg_task = send_telegram_message(int(Variable.get("TG_CHAT_DWH")))
    prev = None
    for table, encrypt_columns, key_owner in tables:
        curr = copy_table_to_dwh.override(  # what the fuck is wrong with black's formatting here...
            task_id=f"copy-encrypted-{table}", outlets=[Dataset(f"STG_USERDATA.encrypted_{table}")]
        )("api_userdata", table, "STG_USERDATA", f"encrypted_{table}", encrypt_columns, key_owner)
        if prev:
            prev >> curr
        prev = curr
        prev >> tg_task
