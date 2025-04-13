-- truncate old state
delete from "ODS_TIMETABLE".ods_link_timetable_group;

insert into "ODS_TIMETABLE".ods_link_timetable_group (
    id,
    event_id,
    group_id
)
select 
    gen_random_uuid(),
    event_id,
    group_id
    from(
        select
            event.id as event_id,
            "group".id as group_id,
        -- оконка чтобы отобрать самое лучшее совпадение по триграмме
        row_number() 
        over (
            partition by event.id -- чтобы потом дедублицировать
            -- подробнее про метод: https://habr.com/ru/companies/otus/articles/770674/
            order by to_tsvector('russian', event.name) @@ plainto_tsquery('russian', "group".group_number) desc
        ) as rn
        from "ODS_TIMETABLE".ods_timetable_act as event
        cross join "DM_TIMETABLE".dim_group_act as "group"
        where event.name ilike '%' || "group".group_number|| '%'
    )
    where rn = 1
