import os
from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.features import get_sql_code

with DAG(
    dag_id="DWH_RENTAL.event",
    schedule=[Dataset("ODS_RENTAL.event")],
    start_date=datetime(2024, 4, 18),
    catchup=False,
    tags=["dwh", "rental", "event"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="event.sql",
        task_id="execute_query",
        doc_md=get_sql_code("event.sql", os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("ODS_RENTAL.event")],
        outlets=[Dataset("DWH_RENTAL.event")],
    )
