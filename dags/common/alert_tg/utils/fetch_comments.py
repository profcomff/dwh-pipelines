import logging

import requests

from dags.common.alert_tg.config import get_api_url, get_token_auth


def fetch_comments(payload):
    """Запрашивает комментарии и возвращает их в виде списка."""
    api_url = get_api_url()
    headers = {"Authorization": get_token_auth(), "accept": "application/json"}

    response = requests.get(api_url, params=payload, headers=headers)
    logging.error(response, response.text)
    if response.status_code != 200:
        logging.error("Ошибка запроса: %s", response.text)
        return []

    if not response.text.strip():
        logging.error("Пустой ответ от сервера при статусе 200")
        return []

    try:
        return response.json().get("comments", [])
    except ValueError as e:
        logging.error("Ошибка парсинга JSON: %s", e)
        return []
