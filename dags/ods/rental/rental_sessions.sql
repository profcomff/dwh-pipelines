-- truncate old states
delete from "ODS_RENTAL".rental_sessions;

insert into "ODS_RENTAL".rental_sessions(
    uuid,
    api_id,
    user_id,
    item_id,
    admin_open_id,
    admin_close_id,
    reservation_ts,
    start_ts,
    end_ts,
    actual_return_ts,
    status
)
select 
    get_random_uuid() as uuid,
    api_id,
    user_id,
    item_id,
    admin_open_id,
    admin_close_id,
    reservation_ts at time zone "utc" at time zone "Europe/Moscow" as reservation_ts,
    start_ts at time zone "utc" at time zone "Europe/Moscow" as start_ts,
    end_ts at time zone "utc" at time zone "Europe/Moscow" as end_ts,
    actual_return_ts at time zone "utc" at time zone "Europe/Moscow" as actual_return_ts,
    status
from "STG_RATING".rental_sessions
limit 10000001;