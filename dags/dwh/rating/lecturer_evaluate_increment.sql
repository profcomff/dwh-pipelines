--evaluate increment
insert into "DWH_RATING".lecturer
select
    gen_random_uuid(),
    ods.api_id,
    ods.first_name,
    ods.last_name,
    ods.middle_name,
    ods.subject,
    ods.avatar_link,
    ods.timetable_id,
    '{{ ds }}'::Date,
    null,
    0.0,
    ods.mark_weighted,
    ods.mark_kindness_weighted,
    ods.mark_clarity_weighted,
    ods.mark_freebie_weighted
    from "ODS_RATING".lecturer as ods
    left join "DWH_RATING".lecturer as dwh
    on ods.api_id = dwh.api_id
    and dwh.valid_to_dt is null
LIMIT 1000000; -- чтобы не раздуло
