from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="ODS_TIMETABLE.ods_link_timetable_room",
    start_date=datetime(2024, 12, 7),
    schedule=[Dataset("DM_TIMETABLE.dim_room_act")],
    catchup=False,
    tags=["ods", "core", "timetable", "link_timetable_cabinet"],
    default_args={"owner": "mixx3"},
):
    PostgresOperator(
        postgres_conn_id="postgres_dwh",
        sql=dedent(r"""
            -- truncate old state
            delete from "ODS_TIMETABLE".ods_link_timetable_room;

            insert into "ODS_TIMETABLE".ods_link_timetable_room (
                id,
                event_id,
                cabinet_id
            )
            select 
                gen_random_uuid(),
                event_id,
                cabinet_id
                from(
                    select
                        event.id as event_id,
                        room.id as cabinet_id,
                    -- оконка чтобы отобрать самое лучшее совпадение по триграмме
                    row_number() 
                    over (
                        partition by event.id -- чтобы потом дедублицировать
                        -- подробнее про метод: https://habr.com/ru/companies/otus/articles/770674/
                        order by to_tsvector('russian', event.name) @@ plainto_tsquery('russian', room.room_name) desc
                    ) as rn
                    from "ODS_TIMETABLE".ods_timetable_act as event
                    cross join "DM_TIMETABLE".dim_room_act as room
                    where event.name ilike '%' || room.room_name || '%'
                )
            where rn = 1
        """),
        task_id="execute_query",
        inlets=[Dataset("ODS_TIMETABLE.ods_timetable_act"), Dataset("DM_TIMETABLE.dim_room_act")],
        outlets=[Dataset("ODS_TIMETABLE.ods_link_timetable_room")],
    )
