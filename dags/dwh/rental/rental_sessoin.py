from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DWH_RENTAL.rental_session",
    schedule=[Dataset("ODS_RENTAL.rental_session")],
    start_date=datetime(2024, 4, 18),
    catchup=False,
    tags=["dwh", "rental", "rental_session"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="rental_session.sql",
        task_id="execute_query",
        inlets=[Dataset("ODS_RENTAL.rental_session")],
        outlets=[Dataset("DWH_RENTAL.rental_session")],
    )
