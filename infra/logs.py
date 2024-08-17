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


with DAG(
    dag_id="logs_cube",
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("ODS_INFRA_LOGS.container_log")],
    catchup=False,
    tags=["dwh", "dm", "infra", "logs"],
    default_args={"owner": "dyakovri"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            r"""
            with new_log as (
                select
                    coalesce (container_name, 'all') as container_name,
                    coalesce(to_char(date_trunc('day', create_ts), 'YYYY-MM-DD'), 'all') as create_date,
                    coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='DEBUG' then 1 else 0 end), 0) as debug_cnt,
                    coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='WARNING' then 1 else 0 end), 0) as warn_cnt,
                    coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='INFO' then 1 else 0 end), 0) as info_cnt,
                    coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='ERROR' then 1 else 0 end), 0) as error_cnt,
                    coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='CRITICAL' then 1 else 0 end), 0) as critical_cnt,
                    coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}')) <> ALL ('{"DEBUG", "WARNING", "INFO", "ERROR", "CRITICAL"}') then 1 else 0 end), 0) as other_cnt
                from "ODS_INFRA_LOGS".container_log
                where
                    container_name like 'com_profcomff_api_%'
                    and upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}')) is not null
                group by cube(container_name, date_trunc('day', create_ts))
            )
            merge into "DM_INFRA_LOGS".container_log_cube as clc
            using new_log as nl
                on nl.container_name = clc.container_name and nl.create_date = clc.create_date
            when matched and not (nl.container_name = 'all' and nl.create_date = 'all') then
                update set
                    debug_cnt = greatest(clc.debug_cnt, nl.debug_cnt),
                    warn_cnt = greatest(clc.warn_cnt, nl.warn_cnt),
                    info_cnt = greatest(clc.info_cnt, nl.info_cnt),
                    error_cnt = greatest(clc.error_cnt, nl.error_cnt),
                    critical_cnt = greatest(clc.critical_cnt, nl.critical_cnt),
                    other_cnt = greatest(clc.other_cnt, nl.other_cnt)
            when not matched and not (nl.container_name = 'all' and nl.create_date = 'all') then
                insert (container_name, create_date, debug_cnt, warn_cnt, info_cnt, error_cnt, critical_cnt, other_cnt)
                values (nl.container_name, nl.create_date, nl.debug_cnt, nl.warn_cnt, nl.info_cnt, nl.error_cnt, nl.critical_cnt, nl.other_cnt);
        """
        ),
        task_id="execute_merge_statement",
        inlets=[Dataset("ODS_INFRA_LOGS.container_log")],
        outlets=[Dataset("DM_INFRA_LOGS.container_log_cube")],
    )
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
        sql=dedent(r"""
                    with sq as (
                        select
                            record->>'message' as e_msg,
                            container_name,
                            create_ts
                        from "ODS_INFRA_LOGS".container_log
                        where
                            record->>'level_name' = 'ERROR' or record->>'level_name' = 'CRITICAL') 
                    merge into "DM_INFRA_LOGS".incident_hint as ne 
                    using sq as e 
                        on (ne.container_name = e.container_name) and (ne.message = e.e_msg) and (ne.create_ts = e.create_ts)
                    when not matched then 
                        insert (id,msk_record_loaded_dttm,container_name,message,create_ts)
                        values (DEFAULT,now(),e.container_name,e.e_msg,e.create_ts)"""
        ),
        task_id="incident_logs_from_ods_to_dm",
        inlets=[Dataset("ODS_INFRA_LOGS.container_log")],
        outlets=[Dataset("DM_INFRA_LOGS.incident_hint")],)
