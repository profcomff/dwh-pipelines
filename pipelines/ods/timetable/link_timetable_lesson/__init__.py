from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="ODS_TIMETABLE.ods_link_timetable_lesson",
    start_date=datetime(2024, 12, 7),
    schedule=[Dataset("ODS_TIMETABLE.ods_timetable_act")],
    catchup=False,
    tags=["ods", "core", "timetable", "link_timetable_dim_event"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "ODS_TIMETABLE".ods_link_timetable_dim_event;

            insert into "ODS_TIMETABLE".ods_link_timetable_lesson (
                "group",
                event_tr,
                lesson_id
            )
            select 
                "group",
                event_tr,
                lesson_id
                from(
                    select 
                        event."group" as "group",
                        event.id as event_tr,
                        dim_event.id as lesson_id,
                    -- оконка чтобы отобрать самое лучшее совпадение по триграмме
                    row_number() 
                    over (
                        partition by event.id -- чтобы потом дедублицировать
                        -- подробнее про метод: https://habr.com/ru/companies/otus/articles/770674/
                        order by to_tsvector('russian', event.name) @@ plainto_tsquery('russian', dim_event.event_name_text) desc
                    ) as rn
                    from "ODS_TIMETABLE".ods_timetable_act as event
                    cross join (
                        select 
                            max(id) as id,  
                            event_name_text
                        from "DM_TIMETABLE".dim_event_act
                        where source_name = 'profcomff_timetable_api'
                        group by 
                            event_name_text
                    ) as dim_event
                )
            where rn = 1
        """),
        task_id="execute_query",
        inlets=[Dataset("ODS_TIMETABLE.ods_timetable_act"), Dataset("DM_TIMETABLE.dim_event_act")],
        outlets=[Dataset("ODS_TIMETABLE.ods_link_timetable_lesson")],
    )
