-- close records
update "DWH_RENTAL".item as item
set valid_to_dt = '{{ ds }}'::Date
where item.uuid NOT IN(
    select dwh.uuid from
        (select 
            uuid,
            api_id,
            type_id,
            is_available,
            type,
        from "DWH_RENTAL".item
        ) as dwh
    join "ODS_RENTAL".item as ods
    on  dwh.uuid = ods.uuid
    and dwh.api_id = ods.api_id
    and dwh.type_id = ods.type_id
    and dwh.is_available = ods.is_available
    and dwh.session_id = ods.session_id
    and dwh.type = ods.type
);

--evaluate increment
insert into "DWH_RENTAL".item
select 
    ods.*,
    '{{ ds }}'::Date,
    null
    from "ODS_RENTAL".item as ods
    full outer join "DWH_RENTAL".item as dwh
        on ods.api_id = dwh.api_id
    where 
        dwh.api_id is NULL
        or dwh.valid_to_dt='{{ ds }}'::Date
LIMIT 10000000;
