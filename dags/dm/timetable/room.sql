-- truncate old state
delete from "DM_TIMETABLE".dim_room_act
where source_name = 'profcomff_timetable_api';

insert into "DM_TIMETABLE".dim_room_act (
    id,
    room_direction_text_type,
    room_api_id,
    room_name,
    room_department,
    source_name
)
select
    gen_random_uuid(),
    direction as room_direction_text_type,
    min(id) as room_api_id,
    name as room_name,
    building as room_department,
    'profcomff_timetable_api' as source_name
    from "STG_TIMETABLE"."room"
where not is_deleted
group by name, building, direction
order by room_api_id
