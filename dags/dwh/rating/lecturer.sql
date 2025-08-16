-- close records lecturer info
update "DWH_RATING".lecturer as lecturer
set valid_to_dt = '{{ ds }}'::Date
where lecturer.api_id NOT IN(
    select dwh.api_id from
        (select
            api_id,
            first_name,
            last_name,
            middle_name,
            subject,
            avatar_link,
            timetable_id,
            mark_weighted,
            mark_kindness_weighted,
            mark_clarity_weighted,
            mark_freebie_weighted
        from "DWH_RATING".lecturer
        where valid_to_dt is null
        ) as dwh
    join "ODS_RATING".lecturer as ods
    on  dwh.api_id = ods.api_id
    and dwh.first_name = ods.first_name
    and dwh.last_name = ods.last_name
    and dwh.middle_name = ods.middle_name
    and dwh.subject is not distinct from ods.subject
    and dwh.avatar_link = ods.avatar_link
    and dwh.timetable_id = ods.timetable_id
    and dwh.mark_weighted = ods.mark_weighted
    and dwh.mark_kindness_weighted = ods.mark_kindness_weighted
    and dwh.mark_clarity_weighted = ods.mark_clarity_weighted
    and dwh.mark_freebie_weighted = ods.mark_freebie_weighted
);


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
    '0',
    ods.mark_weighted,
    ods.mark_kindness_weighted,
    ods.mark_clarity_weighted,
    ods.mark_freebie_weighted
    from "ODS_RATING".lecturer as ods
    left join "DWH_RATING".lecturer as dwh
    on ods.api_id = dwh.api_id
    and dwh.valid_to_dt is null
LIMIT 1000000; -- чтобы не раздуло

-- calculate rank
with ranked as (
    select api_id, ROW_NUMBER() OVER (ORDER BY mark_weighted DESC) as rank 
    from "DWH_RATING".lecturer
    where valid_to_dt is null
)
update "DWH_RATING".lecturer as l
set rank = ranked.rank
from ranked
where l.api_id = ranked.api_id
and l.valid_to_dt is null;
