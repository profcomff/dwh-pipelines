-- truncate old state
delete from "ODS_RATING".comment;

insert into "ODS_RATING".comment (
    uuid,
    api_uuid,
    create_ts,
    update_ts,
    subject,
    text,
    mark_kindness,
    mark_freebie,
    mark_clarity,
    lecturer_id,
    review_status
)
select 
    gen_random_uuid() as uuid,
    uuid as api_uuid,
    create_ts at time zone 'utc' at time zone 'Europe/Moscow' as create_ts,
    update_ts at time zone 'utc' at time zone 'Europe/Moscow' as update_ts,
    subject,
    text,
    mark_kindness,
    mark_freebie,
    mark_clarity,
    lecturer_id,
    review_status
from "STG_RATING".comment
limit 1000001;
