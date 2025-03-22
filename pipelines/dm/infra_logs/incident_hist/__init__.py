from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
        sql=dedent(
            r"""
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
        outlets=[Dataset("DM_INFRA_LOGS.incident_hint")],
    )
