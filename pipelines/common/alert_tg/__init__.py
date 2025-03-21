import json
import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


@task(task_id="send_alert_pending_comments", retries=3)
def send_alert_pending_comments():
    token_bot = str(Variable.get("TGBOT_TOKEN"))
    token_auth = str(Variable.get(""))

    API_URLS = {  # Список Адресов
        "development": "http://localhost:8000/comment",
        "testing": "...",
        "production": "...",
    }
    API_URL: str = API_URLS.get(
        str(Variable.get("_ENVIRONMENT")),
        API_URLS["development"],  # Получаем url через переменную в окружении
    )

    batch_size = 3  # Количество строк в одном батче
    payload = {"limit": batch_size, "offset": 0, "unreviewed": True}
    headers = {"Authorization": token_auth, "accept": "application/json"}

    def fetch_comments():
        """Запрашивает комментарии и возвращает их в виде списка кортежей (uuid, user_id, subject)."""
        response = requests.get(API_URL, params=payload, headers=headers)
        if response.status_code != 200:
            logging.error("Ошибка запроса: %s", response.text)
            return []

        return response.json().get("comments", [])

    while True:
        comments = fetch_comments()
        if not comments:
            logging.info("No pending comments")
            break

        comments_ans = [
            {
                "comment_uuid": comment["uuid"],
                "user_id": f"👤 Автор_id: {comment["user_id"]}",
                "subject": f'💬 Текст: "{comment["subject"]}"',
                "url": f"🔗 {API_URL}/{comment["uuid"]}",
            }
            for comment in comments
        ]

        message_json = json.dumps(comments_ans)
        logging.info(message_json)

        req = requests.post(
            f"https://api.telegram.org/bot{token_bot}/sendMessage",
            json=message_json,
        )
        logging.info("Bot send message status %d (%s)", req.status_code, req.text)
        req.raise_for_status()

        payload["offset"] += batch_size


with DAG(
    dag_id="send_alert_pending_comments",
    start_date=datetime(2022, 1, 1),
    schedule_interval="*/10 * * * *",  # 🔥 КАЖДЫЕ 10 МИНУТ
    catchup=False,
    tags=["dwh", "comments"],
    default_args={"owner": "DROPDATABASE"},
) as dag:
    send_alert_pending_comments()
