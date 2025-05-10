-- close records
update "DWH_RENTAL".event as event
set valid_to_dt = '{{ ds }}'::Date
where event.uuid NOT IN(
    select dwh.uuid from
        (select 
            uuid,
            api_id,
            user_id,
            admin_id,
            session_id,
            action_type,
            details,
            create_ts
        from "DWH_RENTAL".event
        ) as dwh
    join "ODS_RENTAL".event as ods
    on  dwh.uuid = ods.uuid
    and dwh.api_id = ods.api_id
    and dwh.user_id = ods.user_id
    and dwh.admin_id = ods.admin_id
    and dwh.session_id = ods.session_id
    and dwh.action_type = ods.action_type
    and dwh.details = ods.details
    and dwh.create_ts = ods.create_ts
);

--evaluate increment
insert into "DWH_RENTAL".event
select 
    ods.*,
    '{{ ds }}'::Date,
    null
    from "ODS_RENTAL".event as ods
    full outer join "DWH_RENTAL".event as dwh
        on ods.api_id = dwh.api_id
    where 
        dwh.api_id is NULL
        or dwh.valid_to_dt='{{ ds }}'::Date
LIMIT 100000;
