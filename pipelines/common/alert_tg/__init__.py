import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# Список адресов API для разных окружений
API_APP_URLS = {
    "development": "http://localhost:8000/comment",
    "test": "https://api.test.profcomff.com/rating/comment",
    "prod": "https://api.profcomff.com/rating/comment",
}


def get_env_variable(name: str, default=None):
    return str(Variable.get(name, default))


def get_api_url():
    environment = get_env_variable("_ENVIRONMENT", "development")
    return API_APP_URLS.get(environment, API_APP_URLS["development"])


def get_token_bot():
    return get_env_variable("TGBOT_TOKEN")


def get_token_auth():
    return get_env_variable("TOKEN_ROBOT_TIMETABLE")


def get_telegram_chat_id():
    return get_env_variable("TG_CHAT_PENDING_COMMENTS")


def fetch_comments(payload):
    """Запрашивает комментарии и возвращает их в виде списка кортежей (uuid, user_id, subject)."""
    api_url = get_api_url()
    headers = {"Authorization": get_token_auth(), "accept": "application/json"}

    response = requests.get(api_url, params=payload, headers=headers)
    if response.status_code != 200:
        logging.error("Ошибка запроса: %s", response.text)
        return []

    return response.json().get("comments", [])


def send_comments(text: str) -> None:
    """Отправляет комментарии в Telegram."""
    token_bot = get_token_bot()
    telegram_url = f"https://api.telegram.org/bot{token_bot}/sendMessage"
    chat_id = get_telegram_chat_id()

    req = requests.post(url=telegram_url, json={"chat_id": int(chat_id), "text": text})
    req.raise_for_status()
    logging.info("Bot send message status %d (%s)", req.status_code, req.text)


batch_size = 5  # Количество строк в одном батче


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
                f"UUID: {comment['uuid']} \n 👤 Автор_id: {comment['user_id']} \n 💬 Текст: \"{comment['subject']}\" \n 🔗 {config.get_api_url().replace('api', 'app', 1)}/{comment['uuid']}"
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
            send_comments(
                f"TEST: {count_comments} новых комметариев"
            )  # Отправка в бота
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
