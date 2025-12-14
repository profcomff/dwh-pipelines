import datetime
import logging

import requests as r
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import get_sql_code


start_year = 2025
start_month = 9
start_day = 6


API_URL = "https://api.234567оprofcomff.com/lecturer/import_rating"


def patch_lecturer_rating_backend(lecturer_with_rating: list[dict]):
    try:
        response = r.patch(
            url=API_URL,
            headers={
                "Authorization": f"token {Variable.get('TOKEN_ROBOT_RATING')}",
            },
            json=lecturer_with_rating,
        )
        if response.status_code == 200:
            logging.info("Rating copied to backend succesfully!")
        else:
            logging.error(
                f"Rating copy to backend failed with code: {response.status_code}\n Response text: {response.text}"
            )
    except Exception as e:
        logging.error(f"Error sending data to backend: {str(e)}")


def get_lecturers_with_rating_from_dwh() -> list[dict]:
    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
            SELECT * FROM DWH_RATING.lecturer as l
            WHERE l.valid_to_dt is NULL
            """
            )
            results = cursor.fetchall()
            lecturers_with_rating = []
            for result in results:
                lecturers_with_rating.append(
                    {
                        "id": int(result[0]),
                        "first_name": str(result[1]),
                        "last_name": str(result[2]),
                        "middle_name": str(result[3]),
                        "avatar_link": str(result[4]) if result[4] is not None else None,
                        "timetable_id": int(result[5]),
                        "mark_weighted": float(result[6]),
                        "mark_kindness_weighted": float(result[7]),
                        "mark_clarity_weighted": float(result[8]),
                        "mark_freebie_weighted": float(result[9]),
                        "rank": int(result[10]),
                    }
                )

            logging.info(f"Took {len(lecturers_with_rating)} lecturers from dwh database")
            return lecturers_with_rating

        except Exception as e:
            logging.error(f"Error ocured while fetching data from dwh db: {str(e)}")
            return []


with DAG(
    dag_id="lecturer_rating_to_backend",
    schedule=[Dataset("DWH_RATING.lecturer")],
    start_date=datetime.datetime(start_year, start_month, start_day),
    catchup=False,
    tags=["dwh", "rating", "lecturer", "backend"],
    default_args={"owner": "VladislavVoskoboinik", "retries": 3, "retry_delay": datetime.timedelta(minutes=5)},
):

    @task
    def get_lecturers():
        return get_lecturers_with_rating_from_dwh()

    @task
    def patch_backend(lecturers: list[dict]):
        return patch_lecturer_rating_backend(lecturer_with_rating=lecturers)

    get_lecturers_task = get_lecturers()
    patch_backend_task = patch_backend(get_lecturers_task)

    get_lecturers_task >> patch_backend_task
