
-- truncate old state
delete from "DM_TIMETABLE".dm_timetable_act;

insert into "DM_TIMETABLE".dm_timetable_act (
    event_id,
    name,
    odd,
    even,
    weekday,
    num,
    "start",
    "end",
    "group",
    group_name,
    event_name,
    room_name,
    lecturer_name
)
select
    event.id,
    event."name",
    event.odd,
    event.even,
    event.weekday,
    event.num,
    event."start",
    event."end",
    event."group",
    dim_group.group_name_text,
    dim_event.event_name_text,
    dim_room.room_name,
    dim_lecturer.lecturer_first_name || ' ' || dim_lecturer.lecturer_middle_name || ' ' || dim_lecturer.lecturer_last_name as lecturer_name
from "ODS_TIMETABLE".ods_timetable_act as event
left join "ODS_TIMETABLE".ods_link_timetable_group as link_group
on event.id = link_group.event_id
left join (
    select 
        id,
        group_api_id,
        group_name_text
    from "DM_TIMETABLE".dim_group_act
    where source_name = 'profcomff_timetable_api'
    ) as dim_group
on dim_group.id = link_group.group_id
left join "ODS_TIMETABLE".ods_link_timetable_lesson as link_event
on event.id = link_event.event_id
left join (
    select 
        id,
        event_api_id,
        event_name_text
    from "DM_TIMETABLE".dim_event_act
    where source_name = 'profcomff_timetable_api'
    ) as dim_event
on link_event.lesson_id = dim_event.id
left join "ODS_TIMETABLE".ods_link_timetable_room as link_room
on event.id = link_room.event_id
left join (
    select
        id,
        room_api_id,
        room_name 
    from "DM_TIMETABLE".dim_room_act
    where source_name = 'profcomff_timetable_api'
) as dim_room
on link_room.room_id = dim_room.id
left join "ODS_TIMETABLE".ods_link_timetable_teacher as link_teacher
on event.id = link_teacher.event_id
left join (
    select
        id,
        lecturer_first_name,
        lecturer_middle_name,
        lecturer_last_name
    from "DM_TIMETABLE".dim_lecturer_act
    where source_name = 'profcomff_timetable_api'
) as dim_lecturer
on link_teacher.teacher_id = dim_lecturer.id;
