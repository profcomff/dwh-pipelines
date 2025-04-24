import logging
from urllib.parse import quote

import requests as r
from airflow.models import Variable


ENVIRONMENT = Variable.get("_ENVIRONMENT")


def get_graph_link(context):
    base_url = "https://airflow.profcomff.com" if ENVIRONMENT == "prod" else "https://airflow.test.profcomff.com"
    dag_id = context["dag"].dag_id
    dag_run_id = context["dag_run"].run_id
    dag_run_id = quote(dag_run_id)
    return f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}&tab=graph"


def send_telegram_message(chat_id, **context):
    url = get_graph_link(context)
    url = url.replace("=", "\\=").replace("-", "\\-").replace("+", "\\+").replace(".", "\\.").replace("_", "\\_")

    token = str(Variable.get("TGBOT_TOKEN"))
    msg = {
        "chat_id": chat_id,
        "text": f"Не все данные удалось скопировать в DWH из БД API\nГраф исполнения: {url}",
        "parse_mode": "MarkdownV2",
    }
    logging.info(msg)
    req = r.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=msg,
    )
    logging.info("Bot send message status %d (%s)", req.status_code, req.text)
    req.raise_for_status()
