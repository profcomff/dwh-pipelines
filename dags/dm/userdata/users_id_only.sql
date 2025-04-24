DELETE FROM "DM_USER".union_member_card;
INSERT INTO "DM_USER".union_member_card
select 
userdata.user_id as userdata_user_id,
um.card_id as card_id,
um.card_number as card_number
from 
(SELECT 
STRING_TO_ARRAY(email, ',') as email_list,
user_id
FROM "DWH_USER_INFO".info
) as userdata
join "STG_UNION_MEMBER".union_member as um
on um.email=any(userdata.email_list)
