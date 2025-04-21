-- truncate old state
delete from "ODS_RENTAL".event;

insert into "ODS_RENTAL".event(
    uuid,
    api_id,
    user_id,
    admin_id,
    session_id,
    action_type,
    details,
    create_ts
)
select
    gen_random_uuid() as uuid,
    api_id,
    admin_id, 
    session_id,
    action_type,
    details,
    create_ts as time zone "utc" at time zone "Europe/Moscow" as create_ts
from "STG_RENTAL".event
limit 1000001;