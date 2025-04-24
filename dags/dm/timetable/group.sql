-- truncate old state
delete from "DM_TIMETABLE".dim_group_act
where source_name = 'profcomff_timetable_api';

insert into "DM_TIMETABLE".dim_group_act (
    id,
    group_api_id,
    group_name_text,
    group_number,
    source_name
)
select
    gen_random_uuid(),
    min(id) as group_api_id,
    number || name as group_name_text,
    number as group_number,
    'profcomff_timetable_api' as source_name
from "STG_TIMETABLE"."group"
where not is_deleted
group by group_name_text, group_number
order by group_api_id
