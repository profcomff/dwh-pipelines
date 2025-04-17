from datetime import timedelta, datetime
from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator
from textwrap import dedent


with DAG(
    dag_id="DM_MARKETING.frontend_actions_services",
    start_date=datetime(2025, 4, 10),
    schedule=[Dataset("ODS_MARKETING.frontend_actions")],
    catchup=False,
    tags=["dm", "marketing"],
    default_args={
        "owner": "VladislavVoskoboinik",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
):
    PostgresOperator(
        task_id="frontend_moves",
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            r"""
            DELETE FROM "DM_MARKETING".frontend_actions_services;
            INSERT INTO "DM_MARKETING".frontend_actions_services
            (uuid, user_id, action, path_from, path_to, user_agent, is_bot, create_ts, service_from_name, service_to_name)
            SELECT
                fa.uuid,
                fa.user_id,
                fa.action,
                fa.path_from,
                fa.path_to,
                fa.user_agent,
                fa.is_bot,
                fa.create_ts,
                fa.name_from,
				fa.name_to
            FROM (
                SELECT 
                    fa.*,
                    b_to.name as name_to,
					b_from.name as name_from
                FROM "ODS_MARKETING".frontend_actions AS fa
                LEFT JOIN "STG_SERVICES".button AS b_to
                    ON SPLIT_PART(fa.path_to::text, '/', -1) = b_to.id::text
				LEFT JOIN "STG_SERVICES".button AS b_from
					ON SPLIT_PART(fa.path_from::text, '/', -1) = b_from.id::text
                WHERE fa.path_to ILIKE '%apps%'
            ) AS fa;
        """
        ),
        inlets=[
            Dataset("STG_SERVICES.button"),
            Dataset("ODS_MARKETING.frontend_actions"),
        ],
        outlets=[Dataset("DM_marketing.frontend_actions_services")],
    )
