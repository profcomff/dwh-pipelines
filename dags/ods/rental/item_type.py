import os
from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.features import get_sql_code


with DAG(
    dag_id="ODS_RENTAL.item_type",
    schedule=[Dataset("STG_RENTAL.item_type")],
    start_date=datetime(2024, 4, 18),
    catchup=False,
    tags=["ods", "rental", "item_type"],
    default_args={"owner": "VladislavVoskoboinik"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="item_type.sql",
        task_id="execute_query",
        doc_md=get_sql_code("item_type.sql", os.path.dirname(os.path.abspath(__file__))),
        inlets=[Dataset("STG_RENTAL.item_type")],
        outlets=[Dataset("ODS_RENTAL.item_type")],
    )
