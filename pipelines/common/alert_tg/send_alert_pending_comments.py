import logging
import datetime

from airflow import DAG
from airflow.decorators import task

from pipelines.common.alert_tg.config import batch_size, get_app_url, get_env_variable, set_env_variable
from pipelines.common.alert_tg.utils.fetch_comments import fetch_comments
from pipelines.common.alert_tg.utils.send_telegram import send_comments


@task(task_id="send_alert_pending_comments", retries=3)
def send_alert_pending_comments():
    last_run_ts = datetime.datetime.fromisoformat(str(get_env_variable("last_run_ts_alert_tg")))  # Получаем время последнего запуска
    set_env_variable("last_run_ts_alert_tg", str(datetime.datetime.today()))  # Устанавливаем время запуска последней проверки

    payload = {"limit": batch_size, "offset": 0, "review_mode": "pending"}
    is_monday = datetime.datetime.today().weekday() == 0
    # now = datetime.datetime.now()
    # yesterday = now - datetime.timedelta(days=1)

    total_today = 0

    if str(get_env_variable("_ENVIRONMENT")) == "prod":
        while True:
            comments, total_unreviewed = fetch_comments(payload)
            if not comments:
                logging.info("No pending comments")
                break

            if is_monday == 0: break  # Выходим и возвращаем только кол-во

            comments_ans = []
            for comment in comments:
                if (not is_monday and datetime.datetime.fromisoformat(comment['update_ts']) >= last_run_ts):  # Смотрим новые комментраии(инкрементальная проверка)
                # if (not is_monday and yesterday <= datetime.datetime.fromisoformat(comment['update_ts']) <= now):
                    comments_ans += [f"UUID: {comment['uuid']} \n 👤 Автор_id: {comment['user_id']} \n 💬 Предмет: \"{comment['subject']}\""]
                    total_today += 1
            if comments_ans:
                send_comments("\n\n".join(comments_ans))  # Отправка в бота
            payload["offset"] += batch_size
        
        result_message = ''
        if not is_monday and total_today:
            result_message += f'Сегодня не проверено: {total_unreviewed} шт.'
        if is_monday:
            result_message += f'Всего непроверенных комментариев: {total_unreviewed} шт.'
        result_message += f'\n🔗 {get_app_url()}'
        send_comments(result_message)

with DAG(
    dag_id="send_alert_pending_comments",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="0 10 * * *",  # каждый день в 10:00
    catchup=False,
    tags=["dwh", "comments"],
    default_args={"owner": "DROPDATABASE"},
) as dag:
    send_alert_pending_comments()
