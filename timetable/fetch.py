import logging
from datetime import datetime, timedelta

import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable

# [[курс, поток, количество групп], ...]
SOURCES = [
    [1, 1, 9],
    [1, 2, 9],
    [2, 1, 9],
    [2, 2, 9],
    [3, 1, 10],
    [3, 2, 8],
    [4, 1, 10],
    [4, 2, 8],
    [5, 1, 13],
    [5, 2, 12],
    [6, 1, 13],
    [6, 2, 11],
]

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)


@task(task_id="download_pages_to_db", outlets=Dataset("STG_RASPHYSMSU.raw_html"))
def download_pages_to_db():
    data = []
    for source in SOURCES:
        for group in range(1, source[2] + 1):
            url = f"http://ras.phys.msu.ru/table/{source[0]}/{source[1]}/{group}.htm"
            response = r.get(url, headers=HEADERS)
            logging.info("Page %s fetched with status %d", url, response.status_code)
            data.append(
                {
                    "url": url,
                    "raw_html": response.text,
                }
            )
    data = pd.DataFrame(data)

    sql_engine = sa.create_engine(DB_DSN)

    sql_engine.execute('TRUNCATE TABLE "STG_RASPHYSMSU".raw_html_old;')
    logging.info("raw_html_old is empty")

    sql_engine.execute(
        'INSERT INTO "STG_RASPHYSMSU".raw_html_old ("url", "raw_html") '
        'SELECT "url", "raw_html" '
        'FROM "STG_RASPHYSMSU".raw_html'
    )
    logging.info("raw_html_old is full")

    sql_engine.execute('TRUNCATE TABLE "STG_RASPHYSMSU".raw_html;')
    logging.info("raw_html is empty")

    data.to_sql(
        "raw_html", sql_engine, schema="STG_RASPHYSMSU", if_exists="append", index=False
    )
    logging.info("raw_html is full")

    return Dataset("STG_RASPHYSMSU.raw_html")


@task(task_id="compare_pages")
def compare_pages():
    sql_engine = sa.create_engine(DB_DSN)
    changed_urls = sql_engine.execute(
        "SELECT COALESCE(new.url, old.url) AS url "
        'FROM "STG_RASPHYSMSU".raw_html AS new '
        'FULL OUTER JOIN "STG_RASPHYSMSU".raw_html_old AS old '
        "    ON new.url = old.url "
        "WHERE old.raw_html IS NULL "
        "    OR new.raw_html IS NULL "
        "    OR old.raw_html <> new.raw_html;"
    ).fetchall()
    changed_urls = [row[0] for row in changed_urls]
    number_of_groups = 0
    for i, source in enumerate(SOURCES):
        number_of_groups += source[2]

    if len(changed_urls) > 0:
        changed_urls_formated = "\n".join(changed_urls[:10])
        token = str(Variable.get("TGBOT_TOKEN"))
        response = r.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": int(Variable.get("TG_CHAT_DWH")),
                "text": "Изменились следующие страницы с расписанием "
                f"({len(changed_urls)} из {number_of_groups}):\n{changed_urls_formated}",
            },
        )
        logging.info(
            "Bot send message status %d (%s)", response.status_code, response.text
        )

    return changed_urls


@dag(
    schedule="0 */1 * * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh", "timetable", "stg"],
    default_args={
        "owner": "dyakovri",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def timetable_download():
    download_pages_to_db() >> compare_pages()


timetable_sync = timetable_download()
