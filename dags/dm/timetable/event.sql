-- truncate old state
delete from "DM_TIMETABLE".dim_event_act
where source_name = 'profcomff_timetable_api';

insert into "DM_TIMETABLE".dim_event_act (
    id,
    event_name_text,
    source_name,
    event_api_id
)
select
    gen_random_uuid(),
    name as event_name_text,
    'profcomff_timetable_api' as source_name,
    min(id) as event_api_id
from "STG_TIMETABLE"."event"
where not is_deleted
group by name
order by event_api_id
