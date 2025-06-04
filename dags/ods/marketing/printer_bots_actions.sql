    TRUNCATE "ODS_MARKETING".printer_bots_actions;
    INSERT INTO "ODS_MARKETING".printer_bots_actions  
SELECT 
gen_random_uuid() as uuid,
action,
path_from,
path_to,
COALESCE(elem->>'status', '')::VARCHAR AS status,
COALESCE(TRY_CAST(elem->>'user_id' AS INT), 0) AS user_id,
COALESCE(elem->>'surname', '')::VARCHAR AS surname,
COALESCE(TRY_CAST(elem->>'number' AS INT), 0) AS number,
COALESCE(TRY_CAST(elem->>'pin' AS INT), 0) AS pin,
COALESCE(TRY_CAST(elem->>'status_code' AS INT), 0) AS status_code,
COALESCE(elem->>'description', NULL)::VARCHAR AS description,
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
user_id = -2;
