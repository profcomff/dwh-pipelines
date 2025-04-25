import logging

import requests as r
import sqlalchemy as sa
from airflow.models import Connection, Variable
from sqlalchemy import create_engine


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
    .replace("?__extra__=%7B%7D", "")
)

DWH_DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)


def get_base_url():
    return "https://airflow.profcomff.com" if ENVIRONMENT == "prod" else "https://airflow.test.profcomff.com"


def prettify_diff(text: str, diff_obj: set):
    for schema in set([obj[0] for obj in diff_obj]):
        tables_diff = set([obj[1] for obj in diff_obj if obj[0] == schema])
        if tables_diff:
            text += f"\n{schema}\n"
        for table in tables_diff:
            text += f"\t{table}\n"
            cols_diff = [obj[2] for obj in diff_obj if obj[0] == schema and obj[1] == table]
            for col in cols_diff:
                text += f"\t\t{col}\n"
        text += "\n"
    return text


def fetch_dwh_db(**context):
    dag_run = context.get("dag_run")
    log_link = dag_run.get_task_instance(context["task_instance"].task_id).log_url
    log_link = log_link.replace("http://localhost:8080", get_base_url())

    api_sql_engine = create_engine(API_DB_DSN)
    dwh_sql_engine = create_engine(DWH_DB_DSN)
    text = ""

    with api_sql_engine.connect() as api_conn, dwh_sql_engine.connect() as dwh_conn:
        api_cols = api_conn.execute(
            sa.text(
                "SELECT table_schema, table_name, column_name "
                "FROM information_schema.columns "
                f"WHERE table_schema IN {tuple([key for key in SCHEMAS.keys()])} AND table_name != 'alembic_version'"
            )
        )

        dwh_cols = dwh_conn.execute(
            sa.text(
                "SELECT table_schema, table_name, column_name "
                "FROM information_schema.columns "
                f"WHERE table_schema IN {tuple([value for value in SCHEMAS.values()])} AND table_name != 'alembic_version'"
            )
        )

        api_cols = set((SCHEMAS[i.table_schema], i.table_name, i.column_name) for i in api_cols)
        dwh_cols = set((i.table_schema, i.table_name, i.column_name) for i in dwh_cols)

        diff_with_api = api_cols - dwh_cols
        diff_with_dwh = dwh_cols - api_cols

    schemas_diff_api = set([obj[0] for obj in diff_with_api if obj[1] != "alembic_version"])
    if len(schemas_diff_api) > 0:
        text += prettify_diff(text="\nДолжны быть в DWH но нет", diff_obj=diff_with_api)

    schemas_diff_dwh = set([obj[0] for obj in diff_with_dwh if obj[1] != "alembic_version"])
    if len(schemas_diff_dwh) > 0:
        text += prettify_diff(text="\nНе должны быть в DWH но есть", diff_obj=diff_with_dwh)

    logging.info(text)
    return schemas_diff_api, schemas_diff_dwh, log_link
