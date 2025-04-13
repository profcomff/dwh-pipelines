-- close records
update "DWH_RATING".comment as comment
set valid_to_dt = '{{ ds }}'::Date
where comment.api_uuid NOT IN(
    select dwh.api_uuid from
        (select 
            api_uuid,
            create_ts,
            update_ts,
            subject,
            text,
            mark_kindness,
            mark_freebie,
            mark_clarity,
            lecturer_id,
            review_status
        from "DWH_RATING".comment
        ) as dwh
    join "ODS_RATING".comment as ods
    on  dwh.api_uuid = ods.api_uuid
    and dwh.create_ts = ods.create_ts
    and dwh.update_ts = ods.update_ts
    and dwh.subject is not distinct from ods.subject  -- primarely null lists
    and dwh.text = ods.text
    and dwh.mark_kindness = ods.mark_kindness
    and dwh.mark_freebie = ods.mark_freebie
    and dwh.mark_clarity = ods.mark_clarity
    and dwh.lecturer_id = ods.lecturer_id
    and dwh.review_status = ods.review_status
);

--evaluate increment
insert into "DWH_RATING".comment
select 
    ods.*,
    '{{ ds }}'::Date,
    null
    from "ODS_RATING".comment as ods
    full outer join "DWH_RATING".comment as dwh
        on ods.api_uuid = dwh.api_uuid
    where 
        dwh.api_uuid is NULL
        or dwh.valid_to_dt='{{ ds }}'::Date
LIMIT 10000000; -- чтобы не раздуло
