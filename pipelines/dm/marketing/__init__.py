from datetime import timedelta, datetime
from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator
from textwrap import dedent


with DAG(
        dag_id="DM_MARKETING.frontend_actions_services",
        start_date=datetime(2025, 4, 10),
        schedule="@once",
        catchup=False,
        tags=["dm", "marketing"],
        default_args={
            "owner": "VladislavVoskoboinik",
            "retries": 3,
            "retry_delay": timedelta(minutes=5)
        }
):
    PostgresOperator(
        task_id="frontend_moves",
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            DELETE FROM "DM_MARKETING".frontend_actions_services;
            INSERT INTO "DM_MARKETING".frontend_actions_services
            (uuid, user_id, action, path_from, path_to, user_agent, is_bot, create_ts, service_name)
            SELECT
                fa.uuid,
                fa.user_id,
                fa.action,
                fa.path_from,
                fa.path_to,
                fa.user_agent,
                fa.is_bot,
                fa.create_ts,
                fa.name
            FROM (
                SELECT 
                    fa.*,
                    b.name 
                FROM "ODS_MARKETING".frontend_actions AS fa
                JOIN "STG_SERVICES".button AS b
                    ON SPLIT_PART(fa.path_to::text, '/', -1) = b.id::text
                WHERE fa.path_to ILIKE '%apps%'
            ) AS fa;
        """),
        inlets=[Dataset("STG_SERVICES.button"), Dataset("ODS_MARKETING.frontend_actions")],
        outlets=[Dataset("DM_marketing.frontend_actions_services")]
    )