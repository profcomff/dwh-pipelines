-- close records
update "DWH_RENTAL".rental_session as rental_session
set valid_to_dt = '{{ ds }}'::Date
where rental_session.uuid NOT IN(
    select dwh.uuid from
        (select 
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
        from "DWH_RENTAL".rental_session
        ) as dwh
    join "ODS_RENTAL".rental_session as ods
    on  dwh.uuid = ods.uuid
    and dwh.api_id = ods.api_id
    and dwh.user_id = ods.user_id
    and dwh.item_id = ods.item_id
    and dwh.admin_open_id = ods.admin_open_id,
    and dwh.admin_close_id = ods.admin_close_id,
    and dwh.reservation_ts = ods.reservation_ts,
    and dwh.start_ts = ods.start_ts,
    and dwh.end_ts = ods.end_ts,
    and dwh.actual_return_ts = ods.actual_return_ts,
    and dwh.status = ods.status
);

--evaluate increment
insert into "DWH_RENTAL".rental_session
select 
    ods.*,
    '{{ ds }}'::Date,
    null
    from "ODS_RENTAL".rental_session as ods
    full outer join "DWH_RENTAL".rental_session as dwh
        on ods.api_id = dwh.api_id
    where 
        dwh.api_id is NULL
        or dwh.valid_to_dt='{{ ds }}'::Date
LIMIT 10000000;
