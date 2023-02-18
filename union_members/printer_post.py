import logging
import requests as r
import pandas as pd
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable, Connection
from datetime import datetime, timedelta


@task(task_id='post_data', retries=3)
def post_data(env):
    table = '"STG_UNION_MEMBER".union_member'
    if env == "prod":
        url = "https://printer.api.profcomff.com/"
        token = str(Variable.get("TOKEN_ROBOT_PRINTER_PROD"))
    else:
        url = "https://printer.api.test.profcomff.com/"
        token = str(Variable.get("TOKEN_ROBOT_PRINTER_TEST"))

    headers = {"Authorization": token}
    con = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")

    users = []

    query = f"""
    SELECT last_name, profcom_id FROM {table}
    WHERE(faculty='Физический факультет') AND(status='Члены Профсоюза')
    """

    data = pd.read_sql_query(query, con)
    for i, row in data.iterrows():
        surname = row['last_name']
        number = row['profcom_id']

        if len(str(number)) > 0 and (not pd.isna(number)):
            user = {
                "username": surname,
                "union_number": number
            }
            users.append(user)

    users_new = {
        "users": users
    }
    resp = r.post(f"{url}is_union_member", json=users_new, headers=headers)
    logging.info(str(resp.json()))
    logging.info("data length: " + str(len(data)))


@dag(
    schedule=[Dataset("STG_UNION_MEMBER.union_member")],
    start_date=datetime(2023, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={
        "owner": "SergeyZamyatin1",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def update_printer_user_list():
    post_data("test")


update_printer_user_list()
