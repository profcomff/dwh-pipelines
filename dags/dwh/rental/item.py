from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="DWH_RENTAL.item",
    schedule=[Dataset("ODS_RENTAL.item")],
    start_date=datetime(2024, 4, 18),
    catchup=False,
    tags=["dwh", "rental", "item"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="item.sql",
        task_id="execute_query",
        inlets=[Dataset("ODS_RENTAL.item")],
        outlets=[Dataset("DWH_RENTAL.item")],
    )
