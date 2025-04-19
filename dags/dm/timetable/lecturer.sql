-- truncate old state
delete from "DM_TIMETABLE".dim_lecturer_act
where source_name = 'profcomff_timetable_api';

insert into "DM_TIMETABLE".dim_lecturer_act (
    id,
    lecturer_api_id,
    lecturer_first_name,
    lecturer_middle_name,
    lecturer_last_name,
    lecturer_avatar_id,
    lecturer_description,
    source_name
)
select
    gen_random_uuid(),
    min(id) as lecturer_api_id,
    first_name,
    middle_name,
    last_name,
    min(avatar_id),
    min(description),
    'profcomff_timetable_api' as source_name
    from "STG_TIMETABLE"."lecturer"
where not is_deleted
group by first_name, middle_name, last_name
order by lecturer_api_id
