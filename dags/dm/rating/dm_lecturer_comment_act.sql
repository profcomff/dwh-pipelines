delete from "DM_RATING".dm_lecturer_comment_act;

insert into "DM_RATING".dm_lecturer_comment_act
select 
    comment.api_uuid as comment_api_uuid,
    lecturer.api_id as lecturer_api_id,
    max(lecturer.first_name || ' ' || lecturer.last_name || ' ' || lecturer.middle_name) as lecturer_full_name,
    max(lecturer.first_name) as first_name,
    max(lecturer.last_name) as last_name,
    max(lecturer.middle_name) as middle_name,
    max(lecturer.timetable_id) as timetable_id,
    (max(lecturer.timetable_id) is distinct from null) as has_timetable_id,
    coalesce(max(lecturer.subject), 'No subject') as lecturer_subject,
    coalesce(max(comment.subject), 'No subject') as comment_subject,
    substring(max(comment.text) for 80) as comment_shortened_text,
    max(comment.text) as comment_full_text,
    max(comment.create_ts) as comment_create_ts,
    max(comment.update_ts) as comment_update_ts,
    max(comment.mark_kindness) as comment_mark_kindness,
    max(comment.mark_freebie) as comment_mark_freebie,
    max(comment.mark_clarity) as comment_mark_clarity,
    max(comment.review_status) as comment_review_status,
    max(link_user_comment.user_id) as user_id,
    max(user_info.full_name) as user_full_name,
    max(user_info.email) as user_email
from "DWH_RATING".comment as comment
    left join "DWH_RATING".lecturer as lecturer
    on comment.lecturer_id = lecturer.api_id
    left join "ODS_RATING".lecturer_user_comment as link_user_comment
    on link_user_comment.lecturer_id = lecturer.api_id
    left join "DWH_USER_INFO".info as user_info
    on link_user_comment.user_id = user_info.user_id
where 1=1
    and lecturer.valid_to_dt is null
    and comment.valid_to_dt is null
group by 
    comment.api_uuid,
    lecturer.api_id;
