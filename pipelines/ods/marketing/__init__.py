from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="ODS_MARKETING_from_STG_MARKETING.actions_info",
    start_date=datetime(2025, 3, 1),
    schedule=[Dataset("ODS.group")],
    catchup=False,
    tags=["ods", "core", "marketing", "frontend"],
    default_args={"owner": "zimovchik"},
):
    frontend_actions = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        TRUNCATE "ODS_MARKETING".frontend_actions;
        INSERT INTO "ODS_MARKETING".frontend_actions            
        SELECT 
    gen_random_uuid() as uuid,
    user_id,
    action,
    path_from,
    path_to,
    COALESCE(elem->>'user_agent', NULL)::VARCHAR AS user_agent,
    coalesce(additional_data ILIKE '%%bot%', FALSE) as is_bot,
    create_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS create_ts
    FROM 
    "STG_MARKETING".actions_info,
    LATERAL (
        SELECT value AS elem
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(additional_data::jsonb) = 'array' THEN additional_data::jsonb
                ELSE jsonb_build_array(additional_data::jsonb)
            END
        )
    ) AS expanded
    WHERE 
    user_id > 0
        """),
        task_id="get_frontend_logs-ODS_MARKETING_fronted_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.frontend_actions")],
    )
    printer_actions = PostgresOperator(postgres_conn_id="postgres_dwh",
        sql=dedent("""
        TRUNCATE "ODS_MARKETING".printer_actions;
        INSERT INTO "ODS_MARKETING".printer_actions    
        SELECT 
    gen_random_uuid() as uuid,
    action,
    path_from,
    path_to,
    COALESCE(elem->>'status', NULL)::VARCHAR AS status,
    COALESCE(elem->>'app_version', NULL)::VARCHAR AS app_version,
    create_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS create_ts
    FROM 
    "STG_MARKETING".actions_info,
    LATERAL (
        SELECT value AS elem
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(additional_data::jsonb) = 'array' THEN additional_data::jsonb
                ELSE jsonb_build_array(additional_data::jsonb)
            END
        )
    ) AS expanded
    WHERE 
    user_id = -1
        """),
        task_id="get_printer_terminal_logs-ODS_MARKETING_printer_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.printer_actions")])
    printer_bots_actions = PostgresOperator(postgres_conn_id="postgres_dwh",
        sql=dedent("""
        TRUNCATE "ODS_MARKETING".printer_bots_actions;
        INSERT INTO "ODS_MARKETING".printer_bots_actions    
        SELECT 
    gen_random_uuid() as uuid,
    action,
    path_from,
    path_to,
    COALESCE(elem->>'status', NULL)::VARCHAR AS status,
    COALESCE(elem->>'app_version', NULL)::VARCHAR AS app_version,
    COALESCE(elem->>'user_id', NULL)::INT AS user_id,
    COALESCE(elem->>'surname', NULL)::VARCHAR AS surname,
    COALESCE(elem->>'number', NULL)::INT AS number,
    COALESCE(elem->>'pin', NULL)::INT AS pin,
    COALESCE(elem->>'status_code', NULL)::INT AS status_code,
    COALESCE(elem->>'description', NULL)::VARCHAR AS description,
    create_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS create_ts
    FROM 
    "STG_MARKETING".actions_info,
    LATERAL (
        SELECT value AS elem
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(additional_data::jsonb) = 'array' THEN additional_data::jsonb
                ELSE jsonb_build_array(additional_data::jsonb)
            END
        )
    ) AS expanded
    WHERE 
    user_id = -2;
        """),
        task_id="get_printer_bots_interactions_logs-ODS_MARKETING_printer_bots_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.printer_bots_actions")])
    rating_actions = PostgresOperator(postgres_conn_id="postgres_dwh",
        sql=dedent("""
        TRUNCATE "ODS_MARKETING".rating_actions;
        INSERT INTO "ODS_MARKETING".rating_actions    
        SELECT 
    gen_random_uuid() as uuid,
    action,
    path_to,
    COALESCE(elem->>'response_status_code', NULL)::INT AS response_status_code,
    COALESCE(elem->>'auth_user_id', NULL)::VARCHAR AS user_id,
    COALESCE(elem->>'query', NULL)::VARCHAR AS query,
    create_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS create_ts
    FROM 
    "STG_MARKETING".actions_info,
    LATERAL (
        SELECT value AS elem
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(additional_data::jsonb) = 'array' THEN additional_data::jsonb
                ELSE jsonb_build_array(additional_data::jsonb)
            END
        )
    ) AS expanded
    WHERE 
    user_id = -3
        """),
        task_id="get_rating_logs-ODS_MARKETING_rating_actions",
        inlets=[Dataset("STG_MARKETING.actions_info")],
        outlets=[Dataset("ODS_MARKETING.rating_actions")])
    frontend_actions >> printer_actions >> printer_bots_actions >> rating_actions
