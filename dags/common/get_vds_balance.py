import logging
from datetime import datetime, timedelta
from functools import partial

import requests as r
import urllib3
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from plugins.features import alert_message


ENVIRONMENT = Variable.get("_ENVIRONMENT")
url = "https://my.vds.sh/manager/billmgr"


@task(task_id="send_telegram_message", retries=3)
def send_telegram_message_or_print(chat_id, balance):
    """Скачать данные из ЛК ОПК"""

    if balance > 700:
        logging.info(f"Баланс {balance} рублей")
    else:
        token = str(Variable.get("TGBOT_TOKEN"))
        r.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": f"Баланс vds.sh составляет {balance} рублей. Время пополнить счет, @Mark_Shidran!",
            },
        )


@task(task_id="fetch_users", retries=3)
def get_balance():
    """Скачать данные из ЛК VDS.sh"""
    urllib3.disable_warnings()

    with r.Session() as session:
        username = str(Variable.get("LK_VDSSH_ADMIN_USERNAME"))
        password = str(Variable.get("LK_VDSSH_ADMIN_PASSWORD"))

        session.get(url, verify=False)

        # Авторизация
        login_data = {'func': 'auth', 'username': username, 'password': password}
        auth_response = session.post(url, data=login_data, verify=False)
        logging.info(f"Auth response status: {auth_response.status_code}")

        # Подтверждение сессии (sok=ok) - обновленное API(?)
        logging.info("Отправляем подтверждение сессии (sok=ok)...")
        confirm_response = session.get(url, params={'sok': 'ok'}, verify=False)
        logging.info(f"Confirm response status: {confirm_response.status_code}")

        # На всякий
        if confirm_response.status_code != 200:
            logging.error("Не удалось подтвердить сессию")
            return None

        # Здесь уже должен быть JSON
        balance_params = {'func': 'desktop', 'startform': 'vds', 'out': 'xjson'}
        balance_response = session.get(url, params=balance_params, verify=False)
        logging.info(f"Balance response status: {balance_response.status_code}")

        # Проверяем ответ баланса на HTML/JSON
        response_text = balance_response.text.strip()

        if not response_text:
            logging.error("Пустой ответ сервера при запросе баланса")
            return None

        if response_text[0] == '<':
            logging.error("Получен HTML вместо JSON при запросе баланса")
            # Выгружаем в логи целиком что получили
            logging.warning(f"Начало ответа: {response_text}")
            return None

        # Парсим баланс
        try:
            balance_data = balance_response.json()
            logging.info(f"Ответ баланса (первые 200 символов): {str(balance_data)[:200]}")

            # Извлечение баланса с проверкой на None
            balance_str = balance_data.get('doc', {}).get('user', {}).get('$balance')

            if balance_str is None:
                logging.error("Ключ баланса не найден в ответе")
                logging.debug(f"Структура ответа: {balance_data}")
                return None

            balance = float(balance_str)
            logging.info(f"Успешно получен баланс: {balance}")
            return balance

        except json.JSONDecodeError as e:
            logging.error(f"Ошибка парсинга JSON баланса: {e}")
            logging.warning(f"Ответ сервера: {response_text[:200]}")
            return None
        except ValueError as e:
            logging.error(f"Ошибка преобразования баланса в число: {e}")
            return None
        except Exception as e:
            logging.error(f"Неожиданная ошибка при получении баланса: {e}")
            return None


with DAG(
    dag_id="check_vds_balance",
    schedule="0 */12 * * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["infra"],
    default_args={
        "owner": "dyakovri",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": partial(alert_message, chat_id=int(Variable.get("TG_CHAT_DWH"))),
    },
) as dag:
    balance = get_balance()
    send_telegram_message_or_print(int(Variable.get("TG_CHAT_MANAGERS")), balance)
