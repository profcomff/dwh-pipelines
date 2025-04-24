from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="ODS_MARKETING.actions_info",
    start_date=datetime(2025, 3, 1),
    schedule=[Dataset("STG_MARKETING.actions_info")],
    catchup=False,
    tags=["ods", "core", "marketing", "frontend"],
    default_args={"owner": "zimovchik"},
):
    frontend_actions = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="frontend_actions.sql",
        task_id="get_frontend_logs-ODS_MARKETING_fronted_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.frontend_actions")],
    )
    printer_actions = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="printer_actions.sql",
        task_id="get_printer_terminal_logs-ODS_MARKETING_printer_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.printer_actions")],
    )
    printer_bots_actions = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="printer_bots_actions.sql",
        task_id="get_printer_bots_interactions_logs-ODS_MARKETING_printer_bots_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.printer_bots_actions")],
    )
    rating_actions = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="rating_actions.sql",
        task_id="get_rating_logs-ODS_MARKETING_rating_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.rating_actions")],
    )
    frontend_actions >> rating_actions>> printer_actions >> printer_bots_actions
