    TRUNCATE "ODS_MARKETING".rating_actions;
    INSERT INTO "ODS_MARKETING".rating_actions    
    SELECT 
gen_random_uuid() as uuid,
action,
path_to,
COALESCE(elem->>'response_status_code', '0')::int AS response_status_code,
(case when elem->>'auth_user_id' != 'Not auth' then elem->>'auth_user_id' else '0' end )::int AS user_id,
COALESCE(elem->>'query', '')::VARCHAR AS query,
create_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS create_ts
FROM 
"STG_MARKETING".actions_info,
LATERAL (
    SELECT value AS elem
    FROM jsonb_array_elements(
        CASE 
            WHEN additional_data = '' THEN jsonb_build_array('{}'::jsonb)
            WHEN jsonb_typeof(additional_data::jsonb) = 'array' THEN additional_data::jsonb
            ELSE jsonb_build_array(additional_data::jsonb)
        END
    )
) AS expanded
WHERE 
user_id = -3
