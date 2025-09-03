import logging

import requests

from dags.common.alert_tg.config import get_token_auth, get_userdata_url


def get_userdata_by_id(user_id: int):
    """Получает объект преподавателя по id."""
    userdata_url = get_userdata_url()
    headers = {"Authorization": get_token_auth(), "accept": "application/json"}

    response = requests.get(userdata_url + "/user/" + user_id, headers=headers)

    logging.info("UserData: " + response.text)

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


def get_user_name_from_userdata(user_id: int):
    if user_id is None:
        return "Anonim"

    for dct in get_userdata_by_id(user_id=user_id)['items']:
        if dct.get("param", None) == "Полное имя":
            return dct['value']
