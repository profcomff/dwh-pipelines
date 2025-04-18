from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from mymsuapi_timetable_task import get_timetable_for_semester_to_db

from plugins.api_utils import get_timetable_for_semester_to_db as get_timetbl

@task(
    task_id="STG_MYMSUAPI.raw_timetable_apir",
    outlets=Dataset("STG_MYMSUAPI.raw_timetable_api"),
)
def get_timetable_for_semester_to_db():
    return get_timetbl()

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
