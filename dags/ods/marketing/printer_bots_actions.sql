TRUNCATE "ODS_MARKETING".printer_bots_actions;
INSERT INTO "ODS_MARKETING".printer_bots_actions  
SELECT 
    gen_random_uuid() as uuid,
    action,
    path_from,
    path_to,
    COALESCE(elem->>'status', '')::VARCHAR AS status,
    COALESCE(elem->>'user_id', '')::VARCHAR AS user_id,
    COALESCE(elem->>'surname', '')::VARCHAR AS surname,
    COALESCE(elem->>'number', '')::VARCHAR AS number,
    CASE 
        WHEN (elem->>'pin') ~ '^[0-9]+$' THEN (elem->>'pin')::INT 
        ELSE 0 
    END AS pin,
    CASE 
        WHEN (elem->>'status_code') ~ '^[0-9]+$' THEN (elem->>'status_code')::INT 
        ELSE NULL 
    END AS status_code,
    COALESCE(elem->>'description', NULL)::VARCHAR AS description,
    create_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS create_ts
FROM 
    "STG_MARKETING".actions_info,
    LATERAL (
        SELECT value AS elem
        FROM jsonb_array_elements(
            CASE  
                WHEN additional_data IS NULL OR additional_data = '' THEN jsonb_build_array('{}'::jsonb)
                WHEN NOT jsonb_valid(additional_data) THEN jsonb_build_array('{}'::jsonb)
                WHEN additional_data ~ '^\{.*\}$' THEN additional_data::jsonb
                WHEN jsonb_typeof(additional_data::jsonb) = 'array' THEN additional_data::jsonb
                ELSE jsonb_build_array(additional_data::jsonb) 
            END
        )
    ) AS expanded
WHERE 
    user_id = -2
    AND action IS NOT NULL;
