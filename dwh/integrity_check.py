import logging
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


@task(task_id="send_telegram_message", retries=3)
def send_telegram_message(chat_id, diff, **context):
    if diff != set():
        dag_run = context.get("dag_run")
        url = dag_run.get_task_instance(context['task_instance'].task_id).replace("=", "\\=").replace("-", "\\-").replace(".", "\\.").replace("_", "\\_")
        token = str(Variable.get("TGBOT_TOKEN"))
        file = f"{datetime.now()}_integrity_check.log"
        req = r.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": f"Проверка соответствия БД выполнена\nЛог отчета: {url}",
                "parse_mode": "MarkdownV2",
            }
        )


@task(task_id="fetch_db", outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_dwh_db():
    schemas = {
        "api_achievement": "STG_ACHIEVEMENT",
        "api_auth": "STG_AUTH",
        "api_marketing": "STG_MARKETING",
        "api_printer": "STG_PRINT",
        "api_design_school": "STG_DESIGN",
        "api_redirector": "STG_REDIRECTOR",
        "api_service": "STG_SERVICES",
        "api_social": "STG_SOCIAL",
        "api_timetable": "STG_TIMETABLE",
        "api_userdata": "STG_USERDATA",
        "bot_tg_print": "STG_PRINT_TG",
        "bot_vk_print": "STG_PRINT_VK",
    }

    api_uri = (
        Connection.get_connection_from_secrets("postgres_api")
        .get_uri()
        .replace("postgres://", "postgresql://")
    )

    dwh_uri = (
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
    )

    api_sql_engine = create_engine(api_uri)
    dwh_sql_engine = create_engine(dwh_uri)

    with api_sql_engine.connect() as api_conn, dwh_sql_engine.connect() as dwh_conn:
        api_cols = api_conn.execute(sa.text(
            f"select table_schema, table_name, column_name from information_schema.columns where table_schema in {tuple([key for key in schemas.keys()])}"))

        dwh_cols = dwh_conn.execute(sa.text(
            f"select table_schema, table_name, column_name from information_schema.columns where table_schema in {tuple([value for value in schemas.values()])}"))

        api_cols = set((schemas[i.table_schema], i.table_name, i.column_name) for i in api_cols)
        dwh_cols = set((i.table_schema, i.table_name, i.column_name) for i in dwh_cols)

        diff = api_cols ^ dwh_cols

    text = ""
    schemas_diff = set([obj[0] for obj in diff])
    for schema in schemas_diff:
        tables_diff = set([obj[1] for obj in diff if obj[0] == schema and obj[1] != "alembic_version"])
        if tables_diff:
            text += f"\n{schema}\n"
        for table in tables_diff:
            text += f"\t{table}\n"
            cols_diff = [obj[2] for obj in diff if obj[0] == schema and obj[1] == table]
            for col in cols_diff:
                text += f"\t\t{col}\n"
        text += "\n"

    logging.info(text)
    return diff

@dag(
    schedule="0 0 */1 * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "dwh", "retries": 3, "retry_delay": timedelta(minutes=5)},
)
def integrity_check():
    result = fetch_dwh_db()
    send_telegram_message(818677727, result)


dwh_integrity_check = integrity_check()
