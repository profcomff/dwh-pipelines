-- close records
update "DWH_RENTAL".strike as strike
set valid_to_dt = '{{ ds }}'::Date
where strike.uuid NOT IN(
    select dwh.uuid from
        (select 
            uuid,
            api_id,
            user_id,
            session_id,
            admin_id,
            reason,
            create_ts
        from "DWH_RENTAL".strike
        ) as dwh
    join "ODS_RENTAL".strike as ods
    on  dwh.uuid = ods.uuid
    and dwh.api_id = ods.api_id
    and dwh.user_id = ods.user_id
    and dwh.session_id = ods.session_id
    and dwh.admin_id = dwh.admin_id
    and dwh.reason = dwh.reason
    and create_ts = dwh.create_ts
);

--evaluate increment
insert into "DWH_RENTAL".strike
select 
    ods.*,
    '{{ ds }}'::Date,
    null
    from "ODS_RENTAL".strike as ods
    full outer join "DWH_RENTAL".strike as dwh
        on ods.api_id = dwh.api_id
    where 
        dwh.api_id is NULL
        or dwh.valid_to_dt='{{ ds }}'::Date
LIMIT 10000000;
