from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="generate_keys",
    start_date=datetime(2025, 4, 24),
    schedule=[Dataset("STG_AUTH.user")],
    catchup=False,
    tags=["dwh", "stg", "encryption"],
    default_args={"owner": "sotov"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            r"""
            -- 128 bit entropy for keys
            INSERT INTO "STG_USERDATA".info_keys 
            SELECT id, encode(gen_random_bytes(16), 'base64') 
            FROM "STG_AUTH".user ON CONFLICT DO NOTHING;"""
        ),
        task_id="generate_keys",
        inlets=[Dataset("STG_AUTH.user")],
        outlets=[Dataset("STG_USERDATA.info_keys")],
    )
