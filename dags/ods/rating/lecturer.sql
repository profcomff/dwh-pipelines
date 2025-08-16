-- truncate old state
delete from "ODS_RATING".lecturer;


insert into "ODS_RATING".lecturer (
    uuid,
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
    mark_freebie_weighted,
    rank
)
WITH grouped_marks AS (
    SELECT
        l.id as lecturer_id,
        l.first_name, 
        l.middle_name, 
        l.last_name,
        COUNT(c.uuid) AS lecturer_comments_num,
        AVG(c.mark_kindness) AS lecturer_mark_kindness_general,
        AVG(c.mark_freebie) AS lecturer_mark_freebie_general,
        AVG(c.mark_clarity) AS lecturer_mark_clarity_general,
        AVG((c.mark_kindness + c.mark_freebie + c.mark_clarity)/3) AS lecturer_mark_general,
        COUNT(c.uuid) + 0.75 AS total_weight
    FROM "STG_RATING".lecturer AS l
    JOIN "STG_RATING".comment AS c ON l.id = c.lecturer_id 
    GROUP BY 
        l.id,
        l.first_name, 
        l.middle_name, 
        l.last_name
),
weighted_marks as (
    select
        lecturer_id,
        first_name,
        middle_name,
        last_name,
        (lecturer_mark_general * lecturer_comments_num + (SELECT AVG((mark_kindness + mark_freebie + mark_clarity)/3) FROM "STG_RATING".comment) * 0.75)/total_weight AS mark_weighted,
        (lecturer_mark_kindness_general * lecturer_comments_num + (SELECT AVG(mark_kindness) FROM "STG_RATING".comment) * 0.75)/total_weight AS mark_kindness_weighted,
        (lecturer_mark_clarity_general * lecturer_comments_num + (SELECT AVG(mark_clarity) FROM "STG_RATING".comment) * 0.75)/total_weight AS mark_clarity_weighted,
        (lecturer_mark_freebie_general * lecturer_comments_num + (SELECT AVG(mark_freebie) FROM "STG_RATING".comment) * 0.75)/total_weight AS mark_freebie_weighted
    from grouped_marks
)
select 
    gen_random_uuid() as uuid,
    l.id as api_id,
    l.first_name,
    l.last_name,
    l.middle_name,
    l.subject,
    l.avatar_link,
    l.timetable_id,
    wm.mark_weighted,
    wm.mark_kindness_weighted,
    wm.mark_clarity_weighted,
    wm.mark_freebie_weighted,
    0 as rank
from "STG_RATING".lecturer as l join weighted_marks as wm
on l.id = wm.lecturer_id
limit 1000001;
