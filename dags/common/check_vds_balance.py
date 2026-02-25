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
    """
    Получение баланса из VDS.sh через API BILLmanager

    Документация по авторизации: https://www.ispsystem.ru/docs/bc/razrabotchiku/rabota-s-api/billmanager-api/klienty-account
    """
    urllib3.disable_warnings()

    username = str(Variable.get("LK_VDSSH_ADMIN_USERNAME"))
    password = str(Variable.get("LK_VDSSH_ADMIN_PASSWORD"))
    url = "https://my.vds.sh/manager/billmgr"

    # Параметры запроса согласно документации BILLmanager API
    params = {
        'authinfo': f'{username}:{password}',  # Авторизация
        'out': 'json',  # Требуем JSON
        'func': 'account',
    }

    logging.info(f"Отправка запроса к {url} с func=account")

    try:
        # Выполняем GET-запрос с параметрами в URL
        response = r.get(url, params=params, verify=False, timeout=30)
        logging.info(f"Статус ответа: {response.status_code}")

        # Проверка HTTP статуса
        if response.status_code != 200:
            logging.error(f"Ошибка HTTP {response.status_code}")
            logging.error(f"Тело ответа: {response.text[:2000]}")  # Ограничение по символам для перестраховочки
            return None

        # Проверяем, не HTML ли это (на всякий случай)
        if response.text.strip().startswith('<'):
            logging.error("Получен HTML вместо JSON. Возможно, неверный метод авторизации или функция.")
            logging.warning(f"Начало ответа: {response.text[:500]}")
            return None

        # Парсим JSON
        try:
            data = response.json()
            logging.warning(f"ПОЛНЫЙ ОТВЕТ ОТ API: {data}")
            logging.info(f"JSON ответ получен. Ключи верхнего уровня: {list(data.keys())}")
            # Логируем структуру ответа (но не весь, чтобы не засорять логи)
            logging.debug(f"Полный ответ: {data}")
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка парсинга JSON: {e}")
            logging.error(f"Первые 500 символов ответа: {response.text[:500]}")
            return None

        # Извлечение баланса
        balance = None
        try:
            balance = float(data['doc']['elem'][0]['balance'])
            logging.info(f"Баланс найден: {balance}")
        except KeyError as e:
            logging.error(f"Отсутствует ожидаемый ключ: {e}")
            return None
        except (IndexError, TypeError):
            logging.error("Нет элементов в списке клиентов или неверная структура")
            return None
        except ValueError:
            logging.error("Баланс не является числом")
            return None

        if balance is None:
            logging.error("Баланс не был получен")
            return None

        logging.info(f"Успешно получен баланс: {balance} руб.")
        return balance

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при выполнении запроса: {e}")
        return None
    except Exception as e:
        logging.error(f"Ошибка: {e}")
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
