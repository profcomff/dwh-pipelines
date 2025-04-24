import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from pipelines.common.alert_tg.config import batch_size, get_api_url, get_env_variable
from pipelines.common.alert_tg.utils.fetch_comments import fetch_comments
from pipelines.common.alert_tg.utils.send_telegram import send_comments


@task(task_id="send_alert_pending_comments", retries=3)
def send_alert_pending_comments():
    payload = {"limit": batch_size, "offset": 0, "unreviewed": True}

    if str(get_env_variable("_ENVIRONMENT")) == "prod":
        while True:
            comments = fetch_comments(payload)
            if not comments:
                logging.info("No pending comments")
                break

            comments_ans = "\n\n".join(
                f"UUID: {comment['uuid']} \n 👤 Автор_id: {comment['user_id']} \n 💬 Текст: \"{comment['subject']}\" \n 🔗 {get_api_url()}/{comment['uuid']}"
                for comment in comments
            )

            send_comments(comments_ans)  # Отправка в бота
            payload["offset"] += batch_size

    else:
        count_comments = 0
        while True:
            comments = fetch_comments(payload)
            if not comments:
                break

            count_comments += len(comments)
            payload["offset"] += batch_size

        if str(get_env_variable("_ENVIRONMENT")) == "test":
            send_comments(f"TEST: {count_comments} новых комметариев")  # Отправка в бота
        else:  # Логика для локального запуска или разработки
            logging.info("Running in local environment")
            print(f"INFO: {count_comments} новых комметариев")
            logging.info(f"INFO: {count_comments} новых комметариев")


with DAG(
    dag_id="send_alert_pending_comments",
    start_date=datetime(2022, 1, 1),
    schedule_interval="*/10 * * * *",  # Каждые 10 минут
    catchup=False,
    tags=["dwh", "comments"],
    default_args={"owner": "DROPDATABASE"},
) as dag:
    send_alert_pending_comments()
