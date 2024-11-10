import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG


with DAG(
    dag_id="DM_MONITORING.db_monitoring_snp",
    start_date = datetime(2024, 11, 10),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["ods", "src", "userdata"],
    description='data weight monitoring',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id="dm_monitoring",
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        INSERT_INTO "DM_MONITORING".db_monitoring_snp
        SELECT
            table_name,
            table_schema,
            pg_size_pretty(table_size) AS table_size,
            pg_size_pretty(indexes_size) AS indexes_size,
            pg_size_pretty(total_size) AS total_size,
            '{{ ds }}'::Date as record_dt
        FROM (
            SELECT
                table_name,
                table_schema,
                pg_table_size(table_name) AS table_size,
                pg_indexes_size(table_name) AS indexes_size,
                pg_total_relation_size(table_name) AS total_size
            FROM (
                SELECT ('"' || table_schema || '"."' || table_name || '"') AS table_name,
                table_schema
                FROM information_schema.tables
            ) AS all_tables
            ORDER BY total_size DESC
        ) AS pretty_sizes;
        LIMIT 100000; -- чтобы не раздуло
        """),
        inlets = [],
        outlets = [Dataset("DM_MONITORING.db_monitoring_snp")],
    )
