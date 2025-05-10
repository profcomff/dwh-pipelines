-- truncate old state
delete from "ODS_RENTAL".event;

insert into "ODS_RENTAL".event(
    uuid,
    user_id,
    admin_id,
    session_id,
    action_type,
    details,
    create_ts,
    api_id
)
select
    gen_random_uuid() as uuid,
    user_id,
    admin_id, 
    session_id,
    action_type,
    details,
    stg_event.create_ts at time zone 'utc' at time zone 'Europe/Moscow' as create_ts,
    id
from "STG_RENTAL".event as stg_event
limit 100000;