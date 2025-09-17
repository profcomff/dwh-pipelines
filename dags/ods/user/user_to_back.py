import datetime
import logging

import requests as r
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


start_year = 2025
start_month = 9
start_day = 17


API_BASE_URL = "https://api.test.profcomff.com/userdata/"


def post_union_members_to_backend(union_members_ids: list):
    succes_rate = {
        "succeed_ids": [],
        "failed_ids": [],
    }
    for union_member_id in union_members_ids:
        data = {
            "items": [{"category": "Учетные данные", "param": "is_union_member", "value": "True"}],
            "source": "opk_db",
        }
        try:
            response = r.post(
                url=API_BASE_URL + f"/user/{union_member_id}",
                headers={
                    "Authorization": f"token {Variable.get('Auth_token_userdata')}",
                },
                json=data,
            )
            if response.status_code == 200:
                succes_rate["succeed_ids"].append(union_member_id)
            else:
                succes_rate["failed_ids"].append(union_member_id)
                logging.error(
                    f"Union member with id {union_member_id} copy to backend failed with code: {response.status_code}\n Response text: {response.text}"
                )
        except Exception as e:
            logging.error(f"Error sending data to backend: {str(e)}")

    logging.info(
        f"{len(succes_rate['succeed_ids'])} union members sent to backend, f{len(succes_rate['failed_ids'])} failed. Failed id`s:{succes_rate['failed_ids']}"
    )


def get_union_members_ids_from_dwh() -> list:
    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
            SELECT * FROM "ODS_USERDATA".student_id as ud
            WHERE ud.is_deleted = False and ud.modified = (
            select MAX(modified) from "ODS_USERDATA".student_id)
            and ud.created = (
            select MAX(created) from "ODS_USERDATA".student_id)
            """
            )
            results = cursor.fetchall()
            user_ids = []
            for result in results:
                user_ids.append(int(result[1]))

            logging.info(f"Took {len(user_ids)} union members from dwh database")
            return user_ids

        except Exception as e:
            logging.error(f"Error ocured while fetching data from dwh db: {str(e)}")
            return []


with DAG(
    dag_id="union_member_to_backend",
    schedule=[Dataset("ODS_USERDATA.student_id")],
    start_date=datetime.datetime(start_year, start_month, start_day),
    catchup=False,
    tags=["ods", "userdata", "union_member", "backend"],
    default_args={"owner": "VladislavVoskoboinik", "retries": 3, "retry_delay": datetime.timedelta(minutes=5)},
):

    @task
    def get_union_members_ids():
        return get_union_members_ids_from_dwh()

    @task
    def patch_backend(um: list):
        return post_union_members_to_backend(union_members_ids=um)

    get_union_members_ids_task = get_union_members_ids()
    patch_backend_task = patch_backend(get_union_members_ids_task)

    get_union_members_ids_task >> patch_backend_task
