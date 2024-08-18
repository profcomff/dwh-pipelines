import logging
from datetime import datetime, timedelta

import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.exceptions import AirflowException

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


def get_group_schedule(
    group_number: int | str,
    course_id: int,
    flow_id: int,
    date_from: datetime,
    date_to: datetime,
):
    url = "https://api.test.my.msu.ru/gateway/public/api/v1/public_content/lessons"
    values_to_keep = [
        "id",
        "date",
        "time_from",
        "time_to",
        "schedule_id",
        "discipline",
        "classroom",
        "conducting_way",
        "lesson_type",
        "teacher_users",
        "study_groups",
    ]
    params = {
        "schedule_id[]": "1",
        "course_id": str(course_id),
        "flow_id": str(flow_id),
        "date_from": date_from.strftime("%Y-%m-%d"),
        "date_to": date_to.strftime("%Y-%m-%d"),
    }
    try:
        response = requests.get(url, params=params)
        logging.info(
            f"Successfully fetched schedule for group {group_number} from {date_from} to {date_to}. status: {response.status_code}"
        )
        if response.status_code == 200:
            lessons = response.json()["result"]["data"]
        else:
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching schedule for group {group_number}: {e}")
        return None
    lessons = filter(
        lambda lesson: (
            (
                lesson["study_groups"][0]["type"] == "simple"
                and lesson["study_groups"][0]["name"] == str(group_number)
            )
        )
        or (
            lesson["study_groups"][0]["type"] == "separation"
            and lesson["study_groups"][0]["base_groups"][0]["name"] == str(group_number)
        )
        or (
            (
                lesson["study_groups"][0]["type"] == "union"
                and any(
                    group["name"] == str(group_number)
                    for group in lesson["study_groups"][0]["base_groups"]
                )
            )
        ),
        lessons,
    )
    lessons = list(lessons)
    res = []
    for lesson in lessons:
        res.append({key: lesson[key] for key in values_to_keep})
    return res


@task(
    task_id="get_timetable_for_semester",
    outlets=Dataset("STG_MYMSUAPI.raw_timetable_api"),
)
def get_timetable_for_semester_to_db():
    data = []
    values_to_keep = [
        "id",
        "date",
        "time_from",
        "time_to",
        "discipline",
        "classroom",
        "conducting_way",
        "lesson_type",
        "teacher_users",
        "study_groups",
    ]
    url = "https://api.test.my.msu.ru/gateway/public/api/v1/public_content/lessons"
    for source in SOURCES:
        params = {
            "schedule_id[]": "1",
            "course_id": str(source[0]),
            "flow_id": str(source[1]),
            "date_from": datetime.now().strftime("%Y-%m-%d"),
            "date_to": (datetime.now() + timedelta.days(130)).strftime("%Y-%m-%d"),
        }
        try:
            response = r.get(url, params=params)
            logging.info(
                "Course %d flow %d timetable fetched with status %d",
                source[0],
                source[1],
                response.status_code,
            )
            if response.status_code == 200:
                result = response.json()["result"]["data"]
                for lesson in result:
                    data.append(
                        {
                            "group_name": lesson["study_groups"][0]["name"],
                            "dicscipline_name": lesson["discipline"]["name"],
                            "discipline_id": lesson["discipline"]["id"],
                            "classroom_name": lesson["classroom"]["name"],
                            "classroom_id": lesson["classroom"]["id"],
                            "lesson_type_text": lesson["lesson_type"]["name"],
                            "lesson_from_dttm_ts": datetime.strptime(
                                f"{lesson['date']} {lesson['time_from']} ",
                                "%Y-%m-%d %H:%M",
                            ),
                            "lesson_to_dttm_ts": datetime.strptime(
                                f"{lesson['date']} {lesson['time_to']} ",
                                "%Y-%m-%d %H:%M",
                            ),
                            "teacher_users": lesson["teacher_users"],
                            "study_groups": lesson["study_groups"],
                        }
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

@dag(
    schedule_interval=None,
    start_date=datetime(2024, 08, 17),
    tags=["dwh", "timetable", "stg"],
    default_args={
        "owner": "zimovchik",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    })
def mymsu_timetable_download():
    get_timetable_for_semester_to_db() 

