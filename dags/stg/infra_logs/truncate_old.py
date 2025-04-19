from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="truncate_old_infra_logs",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 */1 * *",
    catchup=False,
    tags=["dwh", "stg", "infra", "logs"],
    default_args={"owner": "dyakovri"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="""DELETE FROM "STG_INFRA"."container_log" WHERE create_ts < now() - INTERVAL '3 days';""",
        task_id="execute_delete_statement_log",
    )
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql="""DELETE FROM "STG_INFRA"."container_processes" WHERE create_ts < now() - INTERVAL '3 days';""",
        task_id="execute_delete_statement_processes",
    )
