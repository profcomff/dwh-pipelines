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

API_URL = "https://api.test.my.msu.ru/gateway/public/api/v1/"
LESSONS_ROUTE = API_URL + "public_content/lessons"


@task(
    task_id="get_timetable_for_semester",
    outlets=Dataset("STG_MYMSUAPI.raw_timetable_api"),
)
def get_timetable_for_semester_to_db():
    data = []
    for source in SOURCES:
        params = {
            "schedule_id[]": "1",
            "course_id": str(source[0]),
            "flow_id": str(source[1]),
            "date_from": datetime.now().strftime("%Y-%m-%d"),
            "date_to": (datetime.now() + timedelta.days(130)).strftime("%Y-%m-%d"),
        }
        try:
            response = r.get(LESSONS_ROUTE, params=params)
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


@task(
    task_id="flatten_timetable",
    inlets=Dataset("STG_MYMSUAPI.raw_timetable_api"),
    outlets=Dataset("ODS_MYMSUAPI.ods_timetable_api_flattened"),
)
def flatten_timetable():
    sql_engine = sa.create_engine(DB_DSN)
    sql_engine.execute(
        """
        INSERT INTO ODS_MYMSUAPI.ods_timetable_api_flattened (
            group_name,
            discipline_name,
            discipline_id,
            classroom_name,
            classroom_id,
            lesson_type_text,
            lesson_from_dttm_ts,
            lesson_to_dttm_ts,
            teacher_full_name,
            study_group_id,
            study_group_name
        )
        SELECT
            r.group_name,
            r.dicscipline_name,
            r.discipline_id,
            r.classroom_name,
            r.classroom_id,
            r.lesson_type_text,
            r.lesson_from_dttm_ts,
            r.lesson_to_dttm_ts,
            json_build_string(t.first_name, ' ', t.sur_name, ' ', t.second_name) AS teacher_full_name,
            s.id AS study_group_id,
            s.name AS study_group_name
        FROM STG_MYMSUAPI.raw_timetable_api r
        CROSS JOIN LATERAL json_array_elements(r.teacher_users) AS t
        CROSS JOIN LATERAL json_array_elements(r.study_groups) as s;
        """
    )
    return Dataset("ODS_MYMSUAPI.ods_timetable_api_flattened")


@dag(
    dag_id="download_mymsuapi_timetable",
    schedule="@once",
    start_date=datetime(2024, 8, 17),
    tags=["dwh", "timetable", "stg"],
    default_args={
        "owner": "zimovchik",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def mymsu_timetable_download():
    get_timetable_for_semester_to_db() >> flatten_timetable()
