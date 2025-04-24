INSERT INTO "DM_USER".union_member_join
select 
userdata.user_id as userdata_user_id,
userdata.full_name as full_name,
um.first_name as first_name,
um.last_name as last_name,
um.type_of_learning as type_of_learning,
um.rzd_status as wtf_value,
um.academic_level as academic_level,
um.rzd_number as rzd_number,
um.card_id as card_id,
um.card_number as card_number
from 
(SELECT 
STRING_TO_ARRAY(email, ',') as email_list,
user_id,
full_name
FROM "DWH_USER_INFO".info
) as userdata
join "STG_UNION_MEMBER".union_member as um
on um.email=any(userdata.email_list)
ON CONFLICT (full_name)
DO UPDATE SET
    userdata_user_id = EXCLUDED.userdata_user_id,
    full_name = EXCLUDED.full_name,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    type_of_learning = EXCLUDED.type_of_learning,
    wtf_value = EXCLUDED.wtf_value,
    academic_level = EXCLUDED.academic_level,
    rzd_number = EXCLUDED.rzd_number,
    card_id = EXCLUDED.card_id,
    card_number = EXCLUDED.card_number;
