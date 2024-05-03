import logging
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable


ENVIRONMENT = Variable.get("_ENVIRONMENT")
SCHEMAS = {
    "api_achievement": "STG_ACHIEVEMENT",
    "api_auth": "STG_AUTH",
    "api_marketing": "STG_MARKETING",
    "api_printer": "STG_PRINT",
    "api_service": "STG_SERVICES",
    "api_social": "STG_SOCIAL",
    "api_timetable": "STG_TIMETABLE",
    "api_userdata": "STG_USERDATA",
    "bot_tg_print": "STG_PRINT",
    "bot_vk_print": "STG_PRINT",
}

API_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_api")
    .get_uri()
    .replace("postgres://", "postgresql://")
)

DWH_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
)


def get_base_url():
    return 'https://airflow.profcomff.com' if ENVIRONMENT == 'prod' else 'http://airflow.test.profcomff.com'


def prettify_diff(text: str, diff_obj: set):
    for schema in set([obj[0] for obj in diff_obj]):
        tables_diff = set([obj[1] for obj in diff_obj if obj[0] == schema and obj[1] != "alembic_version"])
        if tables_diff:
            text += f"\n{schema}\n"
        for table in tables_diff:
            text += f"\t{table}\n"
            cols_diff = [obj[2] for obj in diff_obj if obj[0] == schema and obj[1] == table]
            for col in cols_diff:
                text += f"\t\t{col}\n"
        text += "\n"
    return text


@task(task_id="send_telegram_message", retries=3)
def send_telegram_message(chat_id, diff):
    diff_l, diff_r, url = diff
    url = url.replace("=", "\\=").replace("-", "\\-").replace(".", "\\.").replace("_", "\\_")
    if diff_l == set() and diff_r == set():
        return
    token = str(Variable.get("TGBOT_TOKEN"))
    req = r.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": f"Таблицы DWH не соответствуют таблицам API\nЛог отчета: {url}",
            "parse_mode": "MarkdownV2",
        }
    )
    req.raise_for_status()
    logging.info("Bot send message status %d (%s)", req.status_code, req.text)


@task(task_id="fetch_db", outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_dwh_db(**context):
    dag_run = context.get("dag_run")
    log_link = dag_run.get_task_instance(context['task_instance'].task_id).log_url
    log_link = log_link.replace("http://localhost:8080", get_base_url())

    api_sql_engine = create_engine(API_DB_DSN)
    dwh_sql_engine = create_engine(DWH_DB_DSN)
    text = ""

    with api_sql_engine.connect() as api_conn, dwh_sql_engine.connect() as dwh_conn:
        api_cols = api_conn.execute(sa.text(
            "SELECT table_schema, table_name, column_name "
            "FROM information_schema.columns "
            f"WHERE table_schema IN {tuple([key for key in SCHEMAS.keys()])}"
        ))

        dwh_cols = dwh_conn.execute(sa.text(
            "SELECT table_schema, table_name, column_name "
            "FROM information_schema.columns "
            f"WHERE table_schema IN {tuple([value for value in SCHEMAS.values()])}"
        ))

        api_cols = set((SCHEMAS[i.table_schema], i.table_name, i.column_name) for i in api_cols)
        dwh_cols = set((i.table_schema, i.table_name, i.column_name) for i in dwh_cols)

        diff_with_api = api_cols - dwh_cols
        diff_with_dwh = dwh_cols - api_cols

    schemas_diff_api = set([obj[0] for obj in diff_with_api])
    if schemas_diff_api:
        text += prettify_diff(text="\nДолжны быть в DWH но нет", diff_obj=diff_with_api)

    schemas_diff_dwh = set([obj[0] for obj in diff_with_dwh])
    if schemas_diff_dwh:
        text += prettify_diff(text="\nНе должны быть в DWH но есть", diff_obj=diff_with_dwh)

    logging.info(text)
    return schemas_diff_api, schemas_diff_dwh, log_link


with DAG(
    dag_id="dwh_integrity_check",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags= ["dwh", "infra"],
    default_args={
        "owner": "roslavtsevsv",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    result = fetch_dwh_db()
    send_telegram_message(int(Variable.get("TG_CHAT_DWH")), result)
