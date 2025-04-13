DELETE FROM "ODS_MYMSUAPI".ods_timetable_api_flattened;
INSERT INTO "ODS_MYMSUAPI".ods_timetable_api_flattened (
    group_name,
    discipline_name,
    discipline_id,
    classroom_name,
    classroom_id,
    lesson_type_text,
    lesson_from_dttm_ts,
    lesson_to_dttm_ts,
    teacher_full_name,
    study_group_id,
    study_group_name
)
SELECT
    r.group_name,
    r.dicscipline_name,
    r.discipline_id,
    r.classroom_name,
    r.classroom_id,
    r.lesson_type_text,
    r.lesson_from_dttm_ts,
    r.lesson_to_dttm_ts,
    REPLACE(cast(t->'sur_name'as text) || ' ' || cast(t->'first_name' as text) || ' ' || cast(t->'second_name'::text as text), '"', '') AS teacher_full_name,
    cast(cast(s->'id' as text) as integer) AS study_group_id,
    s->'name' AS study_group_name
FROM "STG_MYMSUAPI".raw_timetable_api r
CROSS JOIN LATERAL json_array_elements(r.teacher_users::json) AS t
CROSS JOIN LATERAL json_array_elements(r.study_groups::json) as s
