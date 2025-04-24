-- truncate old state
delete from "ODS_RATING".lecturer_user_comment;

insert into "ODS_RATING".lecturer_user_comment (
    uuid,
    api_id,
    lecturer_id,
    user_id,
    create_ts,
    update_ts
)
select 
    gen_random_uuid() as uuid,
    id as api_id,
    lecturer_id,
    user_id,
    create_ts at time zone 'utc' at time zone 'Europe/Moscow' as create_ts,
    update_ts at time zone 'utc' at time zone 'Europe/Moscow' as update_ts
from "STG_RATING".lecturer_user_comment
limit 1000001;
