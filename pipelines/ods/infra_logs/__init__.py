from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="logs_convert",
    start_date=datetime(2024, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["dwh", "ods", "infra", "logs"],
    default_args={"owner": "dyakovri"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            r"""
            delete from "ODS_INFRA_LOGS".container_log;

            with parse_log as (
                select
                    id,
                    (regexp_replace(record::text, '(?<!\\)\\u0000', '', 'g'))::json #>> '{"log"}' as record,
                    regexp_replace(logfile, '^(.+\/)([^\\/]+)(\/[^\\/]+)$', '\2') as container_id,
                    create_ts
                from "STG_INFRA".container_log cl
                where record ~ '^\{".+"\}\s*$'  -- not valid json object
                    and logfile ~ '^(.+\/)([^\\/]+)(\/[^\\/]+)$'
            ),
            parse_record as (
                select
                    id,
                    (regexp_replace(record::text, '(?<!\\)\\u0000', '', 'g'))::json as record,
                    container_id,
                    coalesce(
                        ((regexp_replace(record::text, '(?<!\\)\\u0000', '', 'g'))::json #>> '{timestamp}')::timestamp,
                        create_ts
                    ) as create_ts
                from parse_log
                where record ~ '^\{".+"\}\s*$'  -- not valid json object
            ),
            no_parse_record as (
                select
                    id,
                    ('{"message": ' || to_json(regexp_replace(record::text, '(?<!\\)\\u0000', '', 'g')) || '}')::json as message,
                    container_id,
                    create_ts
                from parse_log
                where record !~ '^\{".+"\}\s*$'
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
        """
        ),
        task_id="execute_insert_statement",
        inlets=[Dataset("STG_INFRA.container_log")],
        outlets=[Dataset("ODS_INFRA_LOGS.container_log")],
    )
