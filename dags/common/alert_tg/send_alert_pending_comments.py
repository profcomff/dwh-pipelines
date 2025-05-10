import datetime
import logging
from functools import partial

from airflow import DAG
from airflow.decorators import task

from dags.common.alert_tg.config import BATCH_SIZE, get_app_url, get_env_variable, set_env_variable
from dags.common.alert_tg.utils.fetch_comments import fetch_comments
from dags.common.alert_tg.utils.send_telegram import send_comments
from plugins.features import alert_message


def process_comments(last_run_ts, is_monday):
    payload = {"limit": BATCH_SIZE, "offset": 0, "unreviewed": True}
    total_today = 0
    total_unreviewed = 0

    while True:
        comments_to_send = []
        comments = fetch_comments(payload)
        if not comments:
            logging.info("No pending comments in current batch.")
            break

        logging.info(f"Fetched {len(comments)} comments in batch with offset {payload['offset']}.")

        if is_monday:
            total_unreviewed += len(comments)
            payload["offset"] += BATCH_SIZE
            continue

        for comment in comments:
            total_unreviewed += 1
            comment_update_ts = datetime.datetime.fromisoformat(comment["update_ts"])
            if comment_update_ts >= last_run_ts:
                comments_to_send.append(
                    f"UUID: {comment['uuid']}\n👤 Автор_id: {comment['user_id']}\n💬 Предмет: \"{comment['subject']}\""
                )
                total_today += 1

        if comments_to_send:
            send_comments("\n\n".join(comments_to_send))
            logging.info(f"Sent {len(comments_to_send)} new comments.")

        payload["offset"] += BATCH_SIZE

    return total_today, total_unreviewed


@task(task_id="send_alert_pending_comments", retries=3)
def send_alert_pending_comments():
    try:
        raw_ts = get_env_variable("last_run_ts_alert_tg", default="2022-01-01T00:00:00")
        last_run_ts = datetime.datetime.fromisoformat(raw_ts)
    except ValueError:
        logging.warning("Invalid last_run_ts_alert_tg, defaulting to 2022-01-01")
        last_run_ts = datetime.datetime(2022, 1, 1)

    set_env_variable("last_run_ts_alert_tg", datetime.datetime.now().isoformat())

    is_monday = datetime.datetime.today().weekday() == 0

    total_today, total_unreviewed = process_comments(last_run_ts, is_monday)

    result_message = ""
    if not is_monday and total_today:
        result_message += f"Сегодня не проверено: {total_today} шт."
    if is_monday and total_unreviewed:
        result_message += f"Всего непроверенных комментариев: {total_unreviewed} шт."

    if result_message.strip():
        result_message += f"\n🔗 {get_app_url()}"
        send_comments(result_message)
        logging.info("Sent summary message.")


with DAG(
    dag_id="send_alert_pending_comments",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="0 10 * * *",  # каждый день в 10:00
    catchup=False,
    tags=["dwh", "comments"],
    default_args={
        "owner": "DROPDATABASE",
        "on_failure_callback": partial(alert_message, chat_id=int(get_env_variable("TG_CHAT_DWH"))),
    },
) as dag:
    send_alert_pending_comments()
