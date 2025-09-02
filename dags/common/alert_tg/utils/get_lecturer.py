import logging

import requests

from dags.common.alert_tg.config import get_api_url, get_token_auth


def get_lecturer_by_id(lecturer_id: int) -> dict | None:
    """Получает объект преподавателя по id."""
    api_url = get_api_url()
    headers = {"Authorization": get_token_auth(), "accept": "application/json"}

    response = requests.get(api_url+"/lecturer/"+lecturer_id, headers=headers)

    logging.info("Lecrurer: "+response.text)

    if response.status_code != 200:
        logging.error("Ошибка запроса: %s", response.text)
        return None

    if not response.text.strip():
        logging.error("Пустой ответ от сервера при статусе 200")
        return None

    try:
        return response.json()
    except ValueError as e:
        logging.error("Ошибка парсинга JSON: %s", e)
        return None
