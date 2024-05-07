from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="truncate_old_infra_logs",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 */1 * *",
    catchup=False,
    tags=["dwh", "stg", "infra"],
    default_args={"owner": "dyakovri"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=
            "DELETE FROM \"STG_INFRA\".\"logs\""
            "WHERE create_ts < now() - INTERVAL '3 days';"
    )
