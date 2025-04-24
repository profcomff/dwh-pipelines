-- truncate old state
delete from "ODS_TIMETABLE".ods_link_timetable_lesson;

insert into "ODS_TIMETABLE".ods_link_timetable_lesson (
    id,
    event_id,
    lesson_id
)
select 
    gen_random_uuid(),
    event_id,
    lesson_id
    from(
        select
            event.id as event_id,
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
                id as id,  
                event_name_text
            from "DM_TIMETABLE".dim_event_act
            where source_name = 'profcomff_timetable_api'
        ) as dim_event
    )
where rn = 1