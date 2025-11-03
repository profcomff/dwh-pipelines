import logging
from urllib.parse import quote

import requests as r
from airflow.models import Variable


ENVIRONMENT = Variable.get("_ENVIRONMENT")


# Создает ссылку на граф дага по контексту
def get_graph_link(context):  # **context -> аргумент context является словарем из параметров дага
    # url к airflow на основе окружения prod/test
    base_url = "https://airflow.profcomff.com" if ENVIRONMENT == "prod" else "https://airflow.test.profcomff.com"
    # Параметры дага берутся по ключам context
    dag_id = context["dag"].dag_id
    dag_run_id = context["dag_run"].run_id
    dag_run_id = quote(dag_run_id)
    # Возвращает ссылку на даг в airflow
    return f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}&tab=graph"


# Отправка сообщения в телеграм группе по ее id
def send_telegram_message(chat_id, **context):  # **context -> аргумент context является словарем из параметров дага
    # Получаем ссылку на даг
    url = get_graph_link(context)
    # Заменяем для экранирования спецсимволов
    url = url.replace("=", "\\=").replace("-", "\\-").replace("+", "\\+").replace(".", "\\.").replace("_", "\\_")

    token = str(Variable.get("TGBOT_TOKEN"))
    # Сообщение с ссылкой на даг (содержание, кроме ссылки не меняется)
    msg = {
        "chat_id": chat_id,
        "text": f"Не все данные удалось скопировать в DWH из БД API\nГраф исполнения: {url}",
        "parse_mode": "MarkdownV2",
    }
    logging.info(msg)
    # Отправка сообщения
    req = r.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=msg,
    )
    logging.info("Bot send message status %d (%s)", req.status_code, req.text)
    req.raise_for_status()
