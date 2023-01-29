import logging
from datetime import datetime, timedelta

import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable

TIMETABLE_PAGES = [
    'http://ras.phys.msu.ru/table/1/1/1.htm',
    'http://ras.phys.msu.ru/table/1/1/2.htm',
    'http://ras.phys.msu.ru/table/1/1/3.htm',
    'http://ras.phys.msu.ru/table/1/1/4.htm',
    'http://ras.phys.msu.ru/table/1/1/5.htm',
    'http://ras.phys.msu.ru/table/1/1/6.htm',
    'http://ras.phys.msu.ru/table/1/2/1.htm',
    'http://ras.phys.msu.ru/table/1/2/2.htm',
    'http://ras.phys.msu.ru/table/1/2/3.htm',
    'http://ras.phys.msu.ru/table/1/2/4.htm',
    'http://ras.phys.msu.ru/table/1/2/5.htm',
    'http://ras.phys.msu.ru/table/1/2/6.htm',
    'http://ras.phys.msu.ru/table/1/3/1.htm',
    'http://ras.phys.msu.ru/table/1/3/2.htm',
    'http://ras.phys.msu.ru/table/1/3/3.htm',
    'http://ras.phys.msu.ru/table/1/3/4.htm',
    'http://ras.phys.msu.ru/table/1/3/5.htm',
    'http://ras.phys.msu.ru/table/1/3/6.htm',
    'http://ras.phys.msu.ru/table/2/1/1.htm',
    'http://ras.phys.msu.ru/table/2/1/2.htm',
    'http://ras.phys.msu.ru/table/2/1/3.htm',
    'http://ras.phys.msu.ru/table/2/1/4.htm',
    'http://ras.phys.msu.ru/table/2/1/5.htm',
    'http://ras.phys.msu.ru/table/2/1/6.htm',
    'http://ras.phys.msu.ru/table/2/2/1.htm',
    'http://ras.phys.msu.ru/table/2/2/2.htm',
    'http://ras.phys.msu.ru/table/2/2/3.htm',
    'http://ras.phys.msu.ru/table/2/2/4.htm',
    'http://ras.phys.msu.ru/table/2/2/5.htm',
    'http://ras.phys.msu.ru/table/2/2/6.htm',
    'http://ras.phys.msu.ru/table/2/3/1.htm',
    'http://ras.phys.msu.ru/table/2/3/2.htm',
    'http://ras.phys.msu.ru/table/2/3/3.htm',
    'http://ras.phys.msu.ru/table/2/3/4.htm',
    'http://ras.phys.msu.ru/table/2/3/5.htm',
    'http://ras.phys.msu.ru/table/2/3/6.htm',
    'http://ras.phys.msu.ru/table/3/1/1.htm',
    'http://ras.phys.msu.ru/table/3/1/2.htm',
    'http://ras.phys.msu.ru/table/3/1/3.htm',
    'http://ras.phys.msu.ru/table/3/1/4.htm',
    'http://ras.phys.msu.ru/table/3/1/5.htm',
    'http://ras.phys.msu.ru/table/3/1/6.htm',
    'http://ras.phys.msu.ru/table/3/1/7.htm',
    'http://ras.phys.msu.ru/table/3/1/8.htm',
    'http://ras.phys.msu.ru/table/3/1/9.htm',
    'http://ras.phys.msu.ru/table/3/1/10.htm',
    'http://ras.phys.msu.ru/table/3/2/1.htm',
    'http://ras.phys.msu.ru/table/3/2/2.htm',
    'http://ras.phys.msu.ru/table/3/2/3.htm',
    'http://ras.phys.msu.ru/table/3/2/4.htm',
    'http://ras.phys.msu.ru/table/3/2/5.htm',
    'http://ras.phys.msu.ru/table/3/2/6.htm',
    'http://ras.phys.msu.ru/table/3/2/7.htm',
    'http://ras.phys.msu.ru/table/3/2/8.htm',
    'http://ras.phys.msu.ru/table/4/1/1.htm',
    'http://ras.phys.msu.ru/table/4/1/2.htm',
    'http://ras.phys.msu.ru/table/4/1/3.htm',
    'http://ras.phys.msu.ru/table/4/1/4.htm',
    'http://ras.phys.msu.ru/table/4/1/5.htm',
    'http://ras.phys.msu.ru/table/4/1/6.htm',
    'http://ras.phys.msu.ru/table/4/1/7.htm',
    'http://ras.phys.msu.ru/table/4/1/8.htm',
    'http://ras.phys.msu.ru/table/4/1/9.htm',
    'http://ras.phys.msu.ru/table/4/1/10.htm',
    'http://ras.phys.msu.ru/table/4/2/1.htm',
    'http://ras.phys.msu.ru/table/4/2/2.htm',
    'http://ras.phys.msu.ru/table/4/2/3.htm',
    'http://ras.phys.msu.ru/table/4/2/4.htm',
    'http://ras.phys.msu.ru/table/4/2/5.htm',
    'http://ras.phys.msu.ru/table/4/2/6.htm',
    'http://ras.phys.msu.ru/table/4/2/7.htm',
    'http://ras.phys.msu.ru/table/4/2/8.htm',
    'http://ras.phys.msu.ru/table/5/1/1.htm',
    'http://ras.phys.msu.ru/table/5/1/2.htm',
    'http://ras.phys.msu.ru/table/5/1/3.htm',
    'http://ras.phys.msu.ru/table/5/1/4.htm',
    'http://ras.phys.msu.ru/table/5/1/5.htm',
    'http://ras.phys.msu.ru/table/5/1/6.htm',
    'http://ras.phys.msu.ru/table/5/1/7.htm',
    'http://ras.phys.msu.ru/table/5/1/8.htm',
    'http://ras.phys.msu.ru/table/5/1/9.htm',
    'http://ras.phys.msu.ru/table/5/1/10.htm',
    'http://ras.phys.msu.ru/table/5/1/11.htm',
    'http://ras.phys.msu.ru/table/5/1/12.htm',
    'http://ras.phys.msu.ru/table/5/1/13.htm',
    'http://ras.phys.msu.ru/table/5/2/1.htm',
    'http://ras.phys.msu.ru/table/5/2/2.htm',
    'http://ras.phys.msu.ru/table/5/2/3.htm',
    'http://ras.phys.msu.ru/table/5/2/4.htm',
    'http://ras.phys.msu.ru/table/5/2/5.htm',
    'http://ras.phys.msu.ru/table/5/2/6.htm',
    'http://ras.phys.msu.ru/table/5/2/7.htm',
    'http://ras.phys.msu.ru/table/5/2/8.htm',
    'http://ras.phys.msu.ru/table/5/2/9.htm',
    'http://ras.phys.msu.ru/table/5/2/10.htm',
    'http://ras.phys.msu.ru/table/5/2/11.htm',
    'http://ras.phys.msu.ru/table/5/2/12.htm',
    'http://ras.phys.msu.ru/table/6/1/1.htm',
    'http://ras.phys.msu.ru/table/6/1/2.htm',
    'http://ras.phys.msu.ru/table/6/1/3.htm',
    'http://ras.phys.msu.ru/table/6/1/4.htm',
    'http://ras.phys.msu.ru/table/6/1/5.htm',
    'http://ras.phys.msu.ru/table/6/1/6.htm',
    'http://ras.phys.msu.ru/table/6/1/7.htm',
    'http://ras.phys.msu.ru/table/6/1/8.htm',
    'http://ras.phys.msu.ru/table/6/1/9.htm',
    'http://ras.phys.msu.ru/table/6/1/10.htm',
    'http://ras.phys.msu.ru/table/6/1/11.htm',
    'http://ras.phys.msu.ru/table/6/1/12.htm',
    'http://ras.phys.msu.ru/table/6/1/13.htm',
    'http://ras.phys.msu.ru/table/6/2/1.htm',
    'http://ras.phys.msu.ru/table/6/2/2.htm',
    'http://ras.phys.msu.ru/table/6/2/3.htm',
    'http://ras.phys.msu.ru/table/6/2/4.htm',
    'http://ras.phys.msu.ru/table/6/2/5.htm',
    'http://ras.phys.msu.ru/table/6/2/6.htm',
    'http://ras.phys.msu.ru/table/6/2/7.htm',
    'http://ras.phys.msu.ru/table/6/2/8.htm',
    'http://ras.phys.msu.ru/table/6/2/9.htm',
    'http://ras.phys.msu.ru/table/6/2/10.htm',
    'http://ras.phys.msu.ru/table/6/2/11.htm',
]
USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 " \
             "(KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")


@task(task_id='download_pages_to_db', outlets=Dataset("STG_TIMETABLE.raw_html"))
def download_pages_to_db():
    data = []
    for url in TIMETABLE_PAGES:
        response = r.get(url, headers=HEADERS)
        logging.info("Page %s fetched with status %d", url, response.status_code)
        data.append({
            'url': url,
            'raw_html': response.text,
        })
    data = pd.DataFrame(data)
    sql_engine = sa.create_engine(DB_URI)

    sql_engine.execute('DROP TABLE IF EXISTS "STG_TIMETABLE".raw_html_old;')
    logging.info("Table raw_html_old dropped")

    sql_engine.execute('ALTER TABLE IF EXISTS "STG_TIMETABLE".raw_html RENAME TO raw_html_old;')
    logging.info("Table raw_html renamed to raw_html_old")

    sql_engine.execute('CREATE TABLE "STG_TIMETABLE".raw_html (url VARCHAR(256), raw_html TEXT);')
    logging.info("New raw_html table created")

    data.to_sql('raw_html', sql_engine, schema='STG_TIMETABLE', if_exists='append', index=False)
    logging.info("raw_html data (%d rows) uploaded", len(data))

    return Dataset("STG_TIMETABLE.raw_html")


@task(task_id='compare_pages')
def compare_pages():
    sql_engine = sa.create_engine(DB_URI)
    changed_urls = sql_engine.execute('''
        SELECT COALESCE(new.url, old.url) AS url
        FROM "STG_TIMETABLE".raw_html AS new
        FULL OUTER JOIN "STG_TIMETABLE".raw_html_old AS old
            ON new.url = old.url
        WHERE old.raw_html IS NULL
            OR new.raw_html IS NULL
            OR old.raw_html <> new.raw_html;
    ''').fetchall()
    changed_urls = [row[0] for row in changed_urls]

    if len(changed_urls) > 0:
        changed_urls_formated = '\n'.join(changed_urls[:10])
        token = str(Variable.get("TGBOT_TOKEN"))
        response = r.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={
                "chat_id": -1001758480664,
                "text": f"Изменились следующие страницы с расписанием "
                        f"({len(changed_urls)} из {len(TIMETABLE_PAGES)}):\n{changed_urls_formated}",
            }
        )
        logging.info("Bot send message status %d (%s)", response.status_code, response.text)

    return changed_urls


@dag(
    schedule='0 */6 * * *',
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags= ["dwh"],
    default_args={
        "owner": "dwh",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def timetable_download():
    download_pages_to_db() >> compare_pages()


timetable_sync = timetable_download()
