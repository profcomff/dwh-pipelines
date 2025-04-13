-- truncate old state
delete from "ODS_TIMETABLE".ods_link_timetable_room;

insert into "ODS_TIMETABLE".ods_link_timetable_room (
    id,
    event_id,
    room_id
)
select 
    gen_random_uuid(),
    event_id,
    room_id
    from(
        select
            event.id as event_id,
            room.id as room_id,
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
