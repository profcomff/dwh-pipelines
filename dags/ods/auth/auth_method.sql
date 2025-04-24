-- close records
update "ODS_AUTH".auth_method as am
set valid_to_dt = '{{ ds }}'::Date
where am.id NOT IN(
    select ods.id from
        (select 
            id,
            user_id,
            auth_method,
            param,
            value,
            create_ts,
            update_ts,
            is_deleted
        from "ODS_AUTH".auth_method
        ) as ods
    join "STG_AUTH".auth_method as stg
    on ods.id = stg.id 
    and ods.user_id = stg.user_id
    and ods.auth_method = stg.auth_method
    and ods.param = stg.param
    and ods.value = stg.value
    and ods.create_ts = stg.create_ts
    and ods.update_ts = stg.update_ts
    and ods.is_deleted = stg.is_deleted
);

--evaluate increment
insert into "ODS_AUTH".auth_method
select 
    stg.*,
    '{{ ds }}'::Date,
    null
    from "STG_AUTH".auth_method as stg
    full outer join "ODS_AUTH".auth_method as ods
        on stg.id = ods.id
    where 
        ods.id is NULL
        or ods.valid_to_dt='{{ ds }}'::Date
LIMIT 100000; -- чтобы не раздуло
