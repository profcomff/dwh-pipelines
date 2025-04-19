from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="incident_logs_from_ods_to_dm",
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("ODS_INFRA_LOGS.container_log")],
    catchup=False,
    tags=["dwh", "dm", "infra", "logs"],
    default_args={"owner": "SofyaFin"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="incident_hist.sql",
        task_id="incident_logs_from_ods_to_dm",
        inlets=[Dataset("ODS_INFRA_LOGS.container_log")],
        outlets=[Dataset("DM_INFRA_LOGS.incident_hint")],
    )
