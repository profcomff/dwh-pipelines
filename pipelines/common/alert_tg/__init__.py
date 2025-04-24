import json
import logging
from datetime import datetime
from textwrap import dedent

import requests
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine


DWH_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)


@task(task_id="send_alert_pending_comments", retries=3)
def send_alert_pending_comments():
    dwh_sql_engine = create_engine(DWH_DB_DSN)
    batch_size = 10  # Количество строк в одном батче

    token = str(Variable.get("TGBOT_TOKEN"))

    with dwh_sql_engine.connect() as dwh_conn:
        result = dwh_conn.execution_options(stream_results=True).execute(
            sa.text(
                """
                SELECT uuid, user_id, subject
                FROM "STG_TIMETABLE".comment
                WHERE review_status = 'PENDING';
            """
            )
        )

        while True:
            body = {
                "comments": [],
            }

            comments = result.fetchmany(batch_size)  # Получаем `batch_size` строк за раз
            if not comments:
                logging.info("No pending comments")
                break

            for comment in comments:  # [(uuid, user_id, subject), ]
                comment_dct = {
                    "comment_uuid": str(comment[0]),
                    "user_id": f"👤 Автор_id: {comment[1]}",
                    "subject": f'💬 Текст: "{comment[2]}"',
                    "url": f"🔗 {None}",
                }
                body["comments"] += [comment_dct]

            message_json = json.dumps(body)
            logging.info(message_json)
            req = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json=message_json,
            )
            logging.info("Bot send message status %d (%s)", req.status_code, req.text)
            req.raise_for_status()


with DAG(
    dag_id="send_alert_pending_comments",
    start_date=datetime(2022, 1, 1),
    schedule_interval="*/10 * * * *",  # 🔥 КАЖДЫЕ 10 МИНУТ
    catchup=False,
    tags=["dwh", "comments"],
    default_args={"owner": "DROPDATABASE"},
) as dag:
    send_alert_pending_comments()
