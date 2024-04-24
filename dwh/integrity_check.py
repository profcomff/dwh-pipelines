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
            "text": "DWH база данных устарела!"
        },
    )
    print(diff)
    file = f"{datetime.now()}_integrity_check.txt"
    with open(file, "w+") as f:
        for obj in diff[0]:
            print(obj)
            f.write(obj)
    


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

    schema_diff = list()
    table_diff = list()

    with api_sql_engine.connect() as api_conn:
        with dwh_sql_engine.connect() as dwh_conn:
            dwh_schemas = list([schema[0] for schema in dwh_conn.execute(sa.text(f'''
                    select nspname from pg_catalog.pg_namespace
                ''')).fetchall() if schema[0] not in deletion])
            for api_schema in schemas.keys():
                diff = "====================== СХЕМЫ ======================\n"
                dwh_schema = schemas[api_schema]
                if dwh_schema not in dwh_schemas:
                    diff += f"Схема: {dwh_schema}\n"
                    tables_diff = [table[1] for table in api_conn.execute(sa.text(
                        f'''select * from pg_tables where schemaname='{api_schema}';'''
                    )).fetchall()]
                    tables_diff.remove('alembic_version')
                    for table in tables_diff:
                        diff += f"\tТаблица: {table}\n"
                        api_col_diff = [column for column in api_conn.execute(sa.text(f'''
                                SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND TABLE_SCHEMA = '{api_schema}';''')).fetchall()]
                        for col in api_col_diff:
                            diff += f"\t\tСтолбец: {col[0]}; Тип: {col[1]} \n"
                    schema_diff.append(diff)
                else:
                    dwh_tables = list([table[1] for table in dwh_conn.execute(sa.text(f'''
                    select * from pg_tables where schemaname='{dwh_schema}';'''))])
                    api_tables = list([table[1] for table in api_conn.execute(sa.text(f'''
                    select * from pg_tables where schemaname='{api_schema}';'''))])
                    api_tables.remove('alembic_version')
                    diff = "====================== ТАБЛИЦЫ И КОЛОНКИ ======================\n"
                    for i in range(len(api_tables)):
                        api_t_struct = [column for column in api_conn.execute(sa.text(f'''
                                SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{api_tables[i]}' AND TABLE_SCHEMA = '{api_schema}';''')).fetchall()]
                        dwh_t_struct = [column for column in dwh_conn.execute(sa.text(f'''
                                SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{api_tables[i]}' AND TABLE_SCHEMA = '{dwh_schema}';''')).fetchall()]
                        if api_tables[i] in dwh_tables:
                            for j in range(len(api_t_struct)):
                                api_column_name = api_t_struct[j][0]
                                api_column_type = api_t_struct[j][1]
                                if api_column_name not in [dwh_col[0] for dwh_col in dwh_t_struct]:
                                    diff += f"Колонка: {api_tables[i]}.{api_column_name} Тип: {api_column_type}"                                                                       
                                else:
                                    dwh_col_type = [dwh_col[1] for dwh_col in dwh_t_struct if dwh_col[0] == api_column_name][0]
                                    if api_column_type != dwh_col_type:
                                        diff += f"Разница типов в таблице {api_tables[i]}\n API:{api_column_type} DWH:{dwh_col_type}"                                                                                  
                        else:
                            diff = f"Таблица: {api_schema}.{api_tables[i]}\n"
                            for col in api_t_struct:
                                diff += f"\tСтолбец: {col[0]}; Тип: {col[1]} \n"                                                                         
                        table_diff.append(diff)
    return schema_diff, table_diff

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
        send_telegram_message(818677727, result)

dwh_integrity_check = integrity_check()
