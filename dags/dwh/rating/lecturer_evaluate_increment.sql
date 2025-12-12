--evaluate increment
insert into "DWH_RATING".lecturer(
    uuid,
    api_id,
    first_name,
    last_name,
    middle_name,
    subject,
    avatar_link,
    timetable_id,
    valid_from_dt,
    valid_to_dt,
    rank,
    mark_weighted,
    mark_kindness_weighted,
    mark_clarity_weighted,
    mark_freebie_weighted
)
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
WHERE
    dwh.api_id IS NULL
    OR ( ods.api_id = dwh.api_id
        AND (
            ods.first_name IS DISTINCT FROM dwh.first_name OR
            ods.last_name IS DISTINCT FROM dwh.last_name OR
            ods.middle_name IS DISTINCT FROM dwh.middle_name OR
            ods.subject IS DISTINCT FROM dwh.subject OR
            ods.avatar_link IS DISTINCT FROM dwh.avatar_link OR
            ods.timetable_id IS DISTINCT FROM dwh.timetable_id OR
            ods.mark_weighted IS DISTINCT FROM dwh.mark_weighted OR
            ods.mark_kindness_weighted IS DISTINCT FROM dwh.mark_kindness_weighted OR
            ods.mark_clarity_weighted IS DISTINCT FROM dwh.mark_clarity_weighted OR
            ods.mark_freebie_weighted IS DISTINCT FROM dwh.mark_freebie_weighted
        )
    )
LIMIT 1000000; -- чтобы не раздуло
