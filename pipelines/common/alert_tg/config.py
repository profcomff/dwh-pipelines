from airflow.models import Variable


batch_size = 5  # Количество строк в одном батче


def get_env_variable(name: str, default=None):
    return str(Variable.get(name, default))


# Список адресов API для разных окружений
API_APP_URLS = {
    "development": "http://localhost:8000/comment",
    "test": "https://api.test.profcomff.com/rating/comment",
    "prod": "https://api.profcomff.com/rating/comment",
}


def get_api_url():
    environment = get_env_variable("_ENVIRONMENT", "development")
    return API_APP_URLS.get(environment, API_APP_URLS["development"])


def get_token_bot():
    return get_env_variable("TGBOT_TOKEN")


def get_token_auth():
    return get_env_variable("TOKEN_ROBOT_TIMETABLE")


def get_telegram_chat_id():
    return get_env_variable("TG_CHAT_PENDING_COMMENTS")
