from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable

from plugins.api_utils import copy_table_to_dwh, send_telegram_message
from plugins.features import alert_message


copy_table_to_dwh = task(task_id="copy_table_to_dwh", trigger_rule="one_done", retries=0)(copy_table_to_dwh)


with DAG(
    dag_id="upload_api_rental",
    start_date=datetime(2024, 4, 18),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "stg", "rental"],
    default_args={
        "owner": "VladislavVoskoboinik",
    },
    on_failure_callback=partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
):
    tables = ("event", "item", "item_type", "rental_sessions", "strike")
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
        if prev:
            prev >> curr
        prev = curr
