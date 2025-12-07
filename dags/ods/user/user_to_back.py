import datetime
import logging
import os

import requests as r
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


start_year = 2025
start_month = 9
start_day = 17
API_BASE_URL = ""
API_BASE_AUTH_URL = ""
API_BASE_GROUP_ID = None
env = Variable.get('_ENVIRONMENT')

match env:
    case "prod":
        API_BASE_URL = "https://api.profcomff.com/userdata/"
        API_BASE_AUTH_URL = "https://api.profcomff.com/auth/"
        API_BASE_GROUP_ID = 22
    case "test":
        API_BASE_URL = "https://api.test.profcomff.com/userdata/"
        API_BASE_AUTH_URL = "https://api.test.profcomff.com/auth/"
        API_BASE_GROUP_ID = 121


def get_phone_number_by_user_ids(user_id: int) -> dict:
    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        result = {"phone_number": "", "card_number": ""}
        try:
            cursor.execute(
                f"""
            select ud.phone_number
            from "ODS_USERDATA".phone_number as ud
            where ud.user_id = {user_id} and ud.is_deleted = FALSE
            order by ud.modified desc, ud.created desc
            limit 1;
            """
            )
            phone_number = str(cursor.fetchall())
            result["phone_number"] = phone_number
            logging.info(f"Took phone_number for {user_id} from dwh database")
            cursor.execute(
                f"""
            select card_number from "ODS_USERDATA".card as c
            where c.user_id = {user_id} and c.is_deleted = FALSE
            order by c.modified desc, c.created desc
            limit 1;
            """
            )
            card_number = str(cursor.fetchall())
            result["card_number"] = card_number
            logging.info(f"Took card_number for {user_id} from dwh database")
            return result

        except Exception as e:
            logging.error(f"Error ocured while collecting phone number and card_number for user {user_id} from dwh db: {str(e)}")
            return result


def post_union_members_to_backend(union_members_ids: list):
    succes_rate = {
        "succeed_ids": [],
        "failed_ids": [],
    }
    for union_member_id in union_members_ids:
        info = get_phone_number_by_user_ids(union_member_id)
        data = {
            "items": [
                {"category": "Учетные данные", "param": "Членство в профсоюзе", "value": "true"},
                {"category": "Контакты", "param": "Номер телефона", "value": str(info['phone_number'])},
                {"category": "Учетные данные", "param": "Номер профсоюзного билета", "value":  str(info['card_number'])},
            ],
            "source": "dwh",
        }
        try:
            response = r.post(
                url=API_BASE_URL + f"user/{union_member_id}",
                headers={
                    "Authorization": f"{Variable.get('TOKEN_ROBOT_USERDATA')}",
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


def get_groups_numbers(user_id: int) -> list:
    try:
        response = r.get(
            url=API_BASE_AUTH_URL + f"user/{user_id}",
            headers={
                "Authorization": f"{Variable.get('TOKEN_ROBOT_AUTH')}",
            },
            params={"info": "groups"},
        )
        if response.status_code == 200:
            data = response.json()
            group_ids = data.get("groups")

            logging.info(f"Found group ids {group_ids} for user {user_id} from auth backend")
            return group_ids
        else:
            logging.error(
                f"Get groups for user {user_id} failed with code: {response.status_code}\n Response text: {response.text}"
            )
            return []

    except Exception as e:
        logging.error(f"Error occurred while collecting group ids for user {user_id} from auth backend: {str(e)}")
        return []


def post_members_to_union_group_to_backend(union_members_ids: list):
    try:
        for union_member in union_members_ids:
            all_groups = get_groups_numbers(union_member)
            all_groups.append(API_BASE_GROUP_ID)
            data = {"groups": all_groups}
            response = r.patch(
                url=API_BASE_AUTH_URL + f"user/{union_member}",
                headers={
                    "Authorization": f"{Variable.get('TOKEN_ROBOT_AUTH')}",
                },
                json=data,
            )
            if response.status_code == 200:
                logging.info(f"Updated group with union members {union_members_ids} from dwh database")
            else:
                logging.error(
                    f"Update group with union members {union_members_ids} copy to backend failed with code: {response.status_code}\n Response text: {response.text}"
                )
    except Exception as e:
        logging.error(f"Error sending data to backend: {str(e)}")


def get_users_from_union_group() -> list:
    try:
        response = r.get(
            url=API_BASE_AUTH_URL + f"group/{API_BASE_GROUP_ID}",
            headers={
                "Authorization": f"{Variable.get('TOKEN_ROBOT_AUTH')}",
            },
            params={"info": "users"},
        )
        if response.status_code == 200:
            data = response.json()
            user_ids = data.get("users")

            logging.info(f"Found {len(user_ids)} users in union group from auth backend: {user_ids}")
            return user_ids
        else:
            logging.error(
                f"Get users from union group failed with code: {response.status_code}\n Response text: {response.text}"
            )
            return []

    except Exception as e:
        logging.error(f"Error occurred while collecting users from union group from auth backend: {str(e)}")
        return []


def remove_non_union_members_from_union_group(union_members_ids: list):
    try:
        users_in_group = get_users_from_union_group()
        for user_id in [id for id in users_in_group if id not in union_members_ids]:
            all_groups = get_groups_numbers(user_id)
            updated_groups = [group for group in all_groups if group != API_BASE_GROUP_ID]
            data = {"groups": updated_groups}
            response = r.patch(
                url=API_BASE_AUTH_URL + f"user/{user_id}",
                headers={
                    "Authorization": f"{Variable.get('TOKEN_ROBOT_AUTH')}",
                },
                json=data,
            )
            if response.status_code == 200:
                logging.info(f"Successfully removed user {user_id} from union group")
            else:
                logging.error(
                    f"Failed to remove user {user_id} from union group. Status code: {response.status_code}, Response: {response.text}"
                )
    except Exception as e:
        logging.error(f"Error occurred while removing non-union members from union group: {str(e)}")


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

    @task
    def patch_to_group_backend(um: list):
        post_members_to_union_group_to_backend(union_members_ids=um)

    @task
    def remove_non_union_members(um: list):
        remove_non_union_members_from_union_group(union_members_ids=um)

    get_union_members_ids_task = get_union_members_ids()
    patch_backend_task = patch_backend(get_union_members_ids_task)
    patch_to_group_backend_task = patch_to_group_backend(get_union_members_ids_task)
    remove_non_union_members_task = remove_non_union_members(get_union_members_ids_task)

    get_union_members_ids_task >> patch_backend_task >> patch_to_group_backend_task >> remove_non_union_members_task
