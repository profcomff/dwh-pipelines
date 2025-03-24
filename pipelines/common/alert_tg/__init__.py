import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


@task(task_id="send_alert_pending_comments", retries=3)
def send_alert_pending_comments():
    token_bot = str(Variable.get("TGBOT_TOKEN"))
    token_auth = str(Variable.get("TOKEN_ROBOT_TIMETABLE"))

    API_APP_URLS = {  # Список Адресов
        "development": "http://localhost:8000/comment",
        "test": "https://api.test.profcomff.com/rating/comment",
        "prod": "https://api.profcomff.com/rating/comment",
    }
    API_APP_URL: str = API_APP_URLS.get(
        str(Variable.get("_ENVIRONMENT")),
        API_APP_URLS["development"],  # Получаем url через переменную в окружении
    )
    API_TG_URL = f"https://api.telegram.org/bot{token_bot}/sendMessage"

    batch_size = 5  # Количество строк в одном батче
    payload = {"limit": batch_size, "offset": 0, "unreviewed": True}
    headers = {"Authorization": token_auth, "accept": "application/json"}

    def fetch_comments():
        """Запрашивает комментарии и возвращает их в виде списка кортежей (uuid, user_id, subject)."""
        response = requests.get(API_APP_URL, params=payload, headers=headers)
        if response.status_code != 200:
            logging.error("Ошибка запроса: %s", response.text)
            return []

        return response.json().get("comments", [])

    def send_comments(url: str, text: str) -> None:
        req = requests.post(
            url=url,
            json={
                "chat_id": int(Variable.get("TG_CHAT_PENDING_COMMENTS")),   # "chat_id": -4631087944 OR -1004631087944
                "text": text,
            },
        )
        req.raise_for_status()
        logging.info("Bot send message status %d (%s)", req.status_code, req.text)


    if str(Variable.get("_ENVIRONMENT")) == "test":
        count_comments = 0
        while True:
            comments = fetch_comments()
            if not comments:
                break

            count_comments += len(comments)
            payload["offset"] += batch_size

        # Отправка в бота
        send_comments(API_TG_URL, 
                      text=f'TEST: {count_comments} новых комметариев')  

    elif str(Variable.get("_ENVIRONMENT")) == "prod":
        while True:
            comments = fetch_comments()
            if not comments:
                logging.info("No pending comments")
                break

            comments_ans = "\n\n".join(
                f"UUID: {comment['uuid']} \n 👤 Автор_id: {comment['user_id']} \n 💬 Текст: \"{comment['subject']}\" \n 🔗 {API_APP_URL.replace('api', 'app', 1)}/{comment['uuid']}"
                for comment in comments
            )

            send_comments(API_TG_URL, comments_ans)  # Отправка в бота
            payload["offset"] += batch_size
    else:
        pass  # Хз че тут написать, какую логику, Если запускать локально


with DAG(
    dag_id="send_alert_pending_comments",
    start_date=datetime(2022, 1, 1),
    schedule_interval="*/10 * * * *",  # 🔥 КАЖДЫЕ 10 МИНУТ
    catchup=False,
    tags=["dwh", "comments"],
    default_args={"owner": "DROPDATABASE"},
) as dag:
    send_alert_pending_comments()
