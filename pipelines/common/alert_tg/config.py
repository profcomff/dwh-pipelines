from airflow.models import Variable


BATCH_SIZE = 5  # Количество строк в одном батче


def set_env_variable(name: str, value=None):
    Variable.set(name, value)


def get_env_variable(name: str, default=None):
    return str(Variable.get(name, default))


# Список адресов API для разных окружений
API_APP_URLS = {
    "test": "https://api.test.profcomff.com/rating/comment",
    "prod": "https://api.profcomff.com/rating/comment",
}
APP_URLS = {
    "test": "https://app.test.profcomff.com/apps/44",
    "prod": "https://app.profcomff.com/apps/62",
}


def get_api_url():
    environment = get_env_variable("_ENVIRONMENT", "test")
    return APP_URLS.get(environment, APP_URLS["test"])


def get_app_url():
    environment = get_env_variable("_ENVIRONMENT", "test")
    return APP_URLS.get(environment, APP_URLS["test"])


def get_token_bot():
    return get_env_variable("TGBOT_TOKEN")


def get_token_auth():
    return get_env_variable("TOKEN_ROBOT_TIMETABLE")


def get_telegram_chat_id():
    return get_env_variable("TG_CHAT_PENDING_COMMENTS")
