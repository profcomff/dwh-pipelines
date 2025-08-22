import logging
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from plugins.api_utils import send_telegram_message
from plugins.features import alert_message


# декорированные функции
send_telegram_message = task(task_id="send_telegram_message", trigger_rule="one_failed")(send_telegram_message)


API_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_api")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)

DWH_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)

api = create_engine(API_DB_DSN)
dwh = create_engine(DWH_DB_DSN)


with DAG(
    dag_id="copy_lecturer_rating",
    start_date=datetime(2025, 8, 23),
    schedule=Dataset("DWH_RATING.lecturer"),
    catchup=False,
    tags=["dwh", "api", "rating", "copy"],
    default_args={
        "owner": "VladislavVoskoboinik",
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
    },
):
    logging.info("Перенос рейтинга преподавателя из DWH_RATING в rating_api")
    with api.connect() as api_conn, dwh.connect() as dwh_conn:
        data = pd.read_sql_query(
            f'SELECT api_id, mark_weighted, mark_kindness_weighted, mark_clarity_weighted, mark_freebie_weighted, rank '
            f'FROM "DWH_RATING".lecturer as lect '
            f'WHERE lect.valid_to_dt is NULL and lect.valid_from_dt = ('
            f'SELECT MAX(valid_from_dt) FROM "DWH_RATING".lecturer)',
            dwh_conn,
            'api_id',
        )

        api_conn.execute('TRUNCATE TABLE "api_rating".lecturer_rating;')
        data.to_sql(name="api_rating.lecturer_rating", con=api_conn, if_exists="append")
        logging.info("Данные рейтинга преподавателей были скопированы с dwh в rating_api")
