-- close records
update "DWH_RENTAL".item_type as item_type
set valid_to_dt = '{{ ds }}'::Date
where item_type.uuid NOT IN(
    select dwh.uuid from
        (select 
            uuid,
            api_id,
            name,
            image_url,
            description
        from "DWH_RENTAL".item_type
        ) as dwh
    join "ODS_RENTAL".item_type as ods
    on  dwh.uuid = ods.uuid
    and dwh.api_id = ods.api_id
    and dwh.name = ods.name
    and dwh.image_url = ods.image_url
    and dwh.description = ods.description
);

--evaluate increment
insert into "DWH_RENTAL".item_type
select 
    ods.*,
    '{{ ds }}'::Date,
    null
    from "ODS_RENTAL".item_type as ods
    full outer join "DWH_RENTAL".item_type as dwh
        on ods.api_id = dwh.api_id
    where 
        dwh.api_id is NULL
        or dwh.valid_to_dt='{{ ds }}'::Date
LIMIT 10000000;
