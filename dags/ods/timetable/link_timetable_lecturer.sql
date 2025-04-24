-- truncate old state
delete from "ODS_TIMETABLE".ods_link_timetable_teacher;

insert into "ODS_TIMETABLE".ods_link_timetable_teacher (
    id,
    event_id,
    teacher_id
)
select
    gen_random_uuid(),
    event_id,
    teacher_id
    from(
        select
            event.id as event_id,
            lecturer.id as teacher_id,
        -- оконка чтобы отобрать самое лучшее совпадение по триграмме
        row_number() 
        over (
            partition by event.id -- чтобы потом дедублицировать
            -- подробнее про метод: https://habr.com/ru/companies/otus/articles/770674/
            order by to_tsvector('russian', event.name) @@ plainto_tsquery('russian', lecturer.lecturer_last_name) desc
        ) as rn -- TODO@mixx3: Подумать, можно ли искать ещё по фио и какая будет разница
        from "ODS_TIMETABLE".ods_timetable_act as event
        cross join "DM_TIMETABLE".dim_lecturer_act as lecturer
        where event.name ilike '%' || lecturer.lecturer_last_name || '%'
    )
    where rn = 1
