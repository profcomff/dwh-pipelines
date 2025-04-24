from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

from plugins.api_utils import get_timetable_for_semester_to_db

get_timetable_for_semester_to_db = task(
    task_id="STG_MYMSUAPI.raw_timetable_apir",
    outlets=Dataset("STG_MYMSUAPI.raw_timetable_api"),
)(get_timetable_for_semester_to_db)

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
    get_timetable_for_semester_to_db()
