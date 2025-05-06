--truncate old state
delete from "ODS_RENTAL".strike;
insert into "ODS_RENTAL".strike(
    uuid,
    api_id,
    user_id,
    session_id,
    admin_id,
    reason,
    create_ts
)
select
    gen_random_uuid() as uuid,
    id,
    user_id,
    session_id,
    admin_id,
    reason,
    stg_strike.create_ts at time zone 'utc' at time zone 'Europe/Moscow' as create_ts
from "STG_RENTAL".strike as stg_strike
limit 1000001;
