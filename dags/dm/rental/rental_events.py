from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DM_RENTAL.rental_events",
    schedule=[Dataset("DWH_RENTAL.event"), Dataset("DWH_RENTAL.item"), Dataset("DWH_RENTAL.item_type"),
                 Dataset("DWH_RENTAL.rental_session"), Dataset("DWH_RENTAL.strike")],
    start_date=datetime(2024, 4, 18),
    catchup=False,
    tags=["dm", "rental", "rental_events"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="rental_events.sql",
        task_id="execute_query",
        inlets=[Dataset("DWH_RENTAL.event"), Dataset("DWH_RENTAL.item"), Dataset("DWH_RENTAL.item_type"),
                 Dataset("DWH_RENTAL.rental_session"), Dataset("DWH_RENTAL.strike")],
        outlets=[Dataset("DM_RENTAL.rental_events")],
    )
