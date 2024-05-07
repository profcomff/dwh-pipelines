from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.decorators import task
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
        task_id="execute_delete_statement",
    )


with DAG(
    dag_id="logs_convert",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 */1 * *",
    catchup=False,
    tags=["dwh", "ods", "infra", "logs"],
    default_args={"owner": "dyakovri"},
):
    truncate = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql='TRUNCATE TABLE "ODS_INFRA_LOGS".container_log;',
        task_id="execute_truncate_statement",
    )

    insert = PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            with parse_log as (
                select
                    id,
                    record::json #>> '{log}' as record,
                    regexp_replace(logfile, '^(.+\/)([^\\/]+)(\/[^\\/]+)$', '\2') as container_id,
                    create_ts
                from "STG_INFRA".container_log cl
                where record ~ '^\{.+\}\s*$'  -- not valid json object
                    and logfile ~ '^(.+\/)([^\\/]+)(\/[^\\/]+)$'
            ),
            parse_record as (
                select
                    id,
                    cast(record as json) as record,
                    container_id,
                    coalesce((cast(record as json) #>> '{timestamp}')::timestamp, create_ts) as create_ts
                from parse_log
                where record ~ '^\{.+\}\s*$'  -- not valid json object
            ),
            no_parse_record as (
                select
                    id,
                    ('{"message": ' || to_json(record) || '}')::json as message,
                    container_id,
                    create_ts
                from parse_log
                where record !~ '^\{.+\}\s*$'
            ),
            union_records as (
                select * from parse_record
                union all
                select * from no_parse_record
            ),
            processes as (
                select
                    split_part(record, '::', 1) as container_id,
                    split_part(record, '::', 2) as image_name,
                    split_part(record, '::', 3) as container_name,
                    split_part(record, '::', 4) as status,
                    to_timestamp(split_part(record, '::', 5), 'YYYY-MM-DD HH24:MI:SS') at time zone 'UTC' as create_ts,
                    record_create_ts
                from (
                    select
                        trim(string_to_table(regexp_replace(record, '^\s*(.*)\s*\^\s*$', '\1'), '^')) as record,
                        create_ts as record_create_ts
                    from "STG_INFRA".container_processes cp
                ) as s
            ),
            processes_last as (
                select distinct on (container_id)
                    container_id,
                    container_name
                from processes
                ORDER BY
                    container_id,
                    record_create_ts DESC
            )
            insert into "ODS_INFRA_LOGS".container_log (
                id,
                record,
                container_name,
                create_ts
            )
            select
                id,
                record,
                container_name,
                create_ts
            from union_records ur
            inner join processes_last pl
                on ur.container_id like '%' || pl.container_id || '%';
        """),
        task_id="execute_insert_statement",
    )

    truncate >> insert
