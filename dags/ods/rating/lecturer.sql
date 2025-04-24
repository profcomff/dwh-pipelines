-- truncate old state
delete from "ODS_RATING".lecturer;

insert into "ODS_RATING".lecturer (
    uuid,
    api_id,
    first_name,
    last_name,
    middle_name,
    subject,
    avatar_link,
    timetable_id
)
select 
    gen_random_uuid() as uuid,
    id as api_id,
    first_name,
    last_name,
    middle_name,
    subject,
    avatar_link,
    timetable_id
from "STG_RATING".lecturer
limit 1000001;  
