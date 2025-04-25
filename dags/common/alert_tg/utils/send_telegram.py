import logging

import requests

from pipelines.common.alert_tg.config import get_telegram_chat_id, get_token_bot


def send_comments(text: str) -> None:
    """Отправляет комментарии в Telegram."""
    token_bot = get_token_bot()
    telegram_url = f"https://api.telegram.org/bot{token_bot}/sendMessage"
    chat_id = get_telegram_chat_id()

    req = requests.post(url=telegram_url, json={"chat_id": int(chat_id), "text": text})
    req.raise_for_status()
    logging.info("Bot send message status %d (%s)", req.status_code, req.text)
