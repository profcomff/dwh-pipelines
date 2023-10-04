import logging
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


@task(task_id="send_telegram_message", retries=3)
def send_telegram_message(chat_id, diff):
    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": "DWH база данных устарела! \n" + diff,
        },
    )


@task(task_id="fetch_db", outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_dwh_db():
    # Все еще кринж
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
        "app_airflow": "STG_AIRFLOW",
        "bot_tg_print": "STG_PRINT_TG",
        "bot_vk_print": "STG_PRINT_VK",
    }

    deletion = ('pg_catalog', 'public', 'pg_toast', 'information_schema')

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

    schema_diff = set()
    table_diff = set()
    column_diff = set()

    with api_sql_engine.connect() as api_conn:
        with dwh_sql_engine.connect() as dwh_conn:
            dwh_schemas = set([schema[0] for schema in dwh_conn.execute(sa.text(f'''
                    select nspname from pg_catalog.pg_namespace
                ''')).fetchall() if schema[0] not in deletion])
            for api_schema in schemas.keys():
                dwh_schema = schemas[api_schema]
                if dwh_schema not in dwh_schemas:
                    schema_diff.add(api_schema)
                else:
                    dwh_tables = set([table[1] for table in dwh_conn.execute(sa.text(f'''
                    select * from pg_tables where schemaname='{dwh_schema}';'''))])
                    api_tables = set([table[1] for table in api_conn.execute(sa.text(f'''
                    select * from pg_tables where schemaname='{api_schema}';'''))])
                    api_tables.remove('alembic_version')
                    if dwh_tables == api_tables:
                        for api_table in api_tables:
                            api_t_struct = set([column for column in api_conn.execute(sa.text(f'''
                                SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{api_table}';'''))])
                            dwh_t_struct = set([column for column in dwh_conn.execute(sa.text(f'''
                                SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{api_table}';'''))])
                            if api_t_struct != dwh_t_struct:
                                column_diff.update(api_t_struct.symmetric_difference(dwh_t_struct))
                    else:
                        table_diff.update(api_tables.symmetric_difference(dwh_tables))
    return schema_diff, table_diff, column_diff

@dag(
    schedule="0 0 */1 * *",
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "dwh", "retries": 3, "retry_delay": timedelta(minutes=5)},
)
def integrity_check():
    result = fetch_dwh_db()
    if result:
        send_telegram_message(-633287506, result)


dwh_integrity_check = integrity_check()
