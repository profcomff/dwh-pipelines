import logging
from datetime import datetime, timedelta
from json import dumps

import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Connection, Variable

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
    task_id="flatten_timetable",
    inlets=Dataset("STG_MYMSUAPI.raw_timetable_api"),
    outlets=Dataset("ODS_MYMSUAPI.ods_timetable_api_flattened"),
)
def flatten_timetable():
    sql_engine = sa.create_engine(DB_DSN)
    sql_engine.execute(
        """
        DELETE FROM "ODS_MYMSUAPI".ods_timetable_api_flattened;
        INSERT INTO "ODS_MYMSUAPI".ods_timetable_api_flattened (
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
            REPLACE(cast(t->'sur_name'as text) || ' ' || cast(t->'first_name' as text) || ' ' || cast(t->'second_name'::text as text), '"', '') AS teacher_full_name,
            cast(cast(s->'id' as text) as integer) AS study_group_id,
            s->'name' AS study_group_name
        FROM "STG_MYMSUAPI".raw_timetable_api r
        CROSS JOIN LATERAL json_array_elements(r.teacher_users::json) AS t
        CROSS JOIN LATERAL json_array_elements(r.study_groups::json) as s
        """
    )
    return Dataset("ODS_MYMSUAPI.ods_timetable_api_flattened")


with DAG(
    dag_id="download_mymsuapi_timetable",
    schedule="50 2 */1 * *",
    start_date=datetime(2024, 8, 27),
    tags=["ods", "src", "mymsuapi"],
    default_args={
        "owner": "zimovchik",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    flatten_timetable()
