import logging

import requests

from pipelines.common.alert_tg.config import get_api_url, get_token_auth


def fetch_comments(payload):
    """Запрашивает комментарии и возвращает их в виде списка кортежей (uuid, user_id, subject)."""
    api_url = get_api_url()
    headers = {"Authorization": get_token_auth(), "accept": "application/json"}

    response = requests.get(api_url, params=payload, headers=headers)
    if response.status_code != 200:
        logging.error("Ошибка запроса: %s", response.text)
        return []

    return response.json().get("comments", [])
