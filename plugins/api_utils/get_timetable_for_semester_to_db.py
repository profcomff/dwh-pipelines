import logging
from datetime import datetime, timedelta
from json import dumps

import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.models import Connection
from airflow.exceptions import AirflowException
from airflow.datasets import Dataset

# [[курс, поток, количество групп], ...]
SOURCES = [
    [1, 1, 6],
    [1, 2, 6],
    [1, 3, 6],
    [2, 1, 6],
    [2, 2, 6],
    [2, 3, 6],
    [3, 1, 10],
    [3, 2, 8],
    [4, 1, 10],
    [4, 2, 8],
    [5, 1, 13],
    [5, 2, 12],
    [6, 1, 13],
    [6, 2, 11],
]

DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)

API_URL = "https://api.test.my.msu.ru/gateway/public/api/v1/"
LESSONS_ROUTE = API_URL + "public_content/lessons"

# функция для download_mymsuapi_timetable

def get_timetable_for_semester_to_db():
    data = []
    for source in SOURCES:
        params = {
            "schedule_id[]": "1",
            "course_id": str(source[0]),
            "flow_id": str(source[1]),
            "date_from": datetime.now().strftime("%Y-%m-%d"),
            "date_to": (datetime.now() + timedelta(days=130)).strftime("%Y-%m-%d"),
        }
        try:
            response = r.get(LESSONS_ROUTE, params=params)
            if response.status_code == 200:
                logging.info(
                    "Course %d flow %d timetable fetched succesfully",
                    source[0],
                    source[1],
                )
                result = response.json()["result"]["data"]
                for lesson in result:
                    try:
                        data.append(
                            {
                                "group_name": lesson["study_groups"][0]["name"],
                                "dicscipline_name": lesson["discipline"]["name"],
                                "discipline_id": lesson["discipline"]["id"],
                                "classroom_name": lesson["classroom"]["name"],
                                "classroom_id": lesson["classroom"]["id"],
                                "lesson_type_text": lesson["lesson_type"]["name"],
                                "lesson_from_dttm_ts": datetime.strptime(
                                    f"{lesson['date']} {lesson['time_from']}",
                                    "%Y-%m-%d %H:%M",
                                ),
                                "lesson_to_dttm_ts": datetime.strptime(
                                    f"{lesson['date']} {lesson['time_to']}",
                                    "%Y-%m-%d %H:%M",
                                ),
                                "teacher_users": dumps(
                                    lesson["teacher_users"], ensure_ascii=False
                                ),
                                "study_groups": dumps(
                                    lesson["study_groups"], ensure_ascii=False
                                ),
                            }
                        )
                    except Exception as e:
                        logging.error(
                            f"Error taking data out: %s for cource %d flow %d group %s",
                            str(e),
                            source[0],
                            source[1],
                            lesson["study_groups"][0]["name"],
                        )
            else:
                logging.error(
                    "Error fetching course %d flow %d timetable: %d",
                    source[0],
                    source[1],
                    response.status_code,
                )
        except r.exceptions.RequestException as e:
            logging.error(
                "Error fetching course %d flow %d timetable: %s",
                source[0],
                source[1],
                e,
            )
            raise AirflowException("Failed to fetch timetable")
    data = pd.DataFrame(data)
    sql_engine = sa.create_engine(DB_DSN)
    data.to_sql(
        "raw_timetable_api",
        con=sql_engine,
        schema="STG_MYMSUAPI",
        if_exists="replace",
        index=False,
    )
    return Dataset("STG_MYMSUAPI.raw_timetable_api")
