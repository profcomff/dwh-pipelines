from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="ODS_RENTAL.event",
    schedule=[Dataset("STG_RENTAL.event")],
    start_date=datetime(2024, 4, 18),
    catchup=False,
    tags=["ods", "rental", "event"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="event.sql",
        task_id="execute_query",
        inlets=[Dataset("STG_RENTAL.event")],
        outlets=[Dataset("ODS_RENTAL.event")],
    )
