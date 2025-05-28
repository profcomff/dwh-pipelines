delete from "ODS_SOCIAL".git_hub;
INSERT INTO "ODS_SOCIAL".git_hub
SELECT
    gen_random_uuid() as uuid,
    ws.message::jsonb->>'action' as status,
    ws.message::jsonb->'issue'->>'url' as issue_url,
    ws.message::jsonb->'issue'->'user'->>'login' as user_login,
    ws.message::jsonb->'issue'->>'title' as issue_title,
    (ws.message::jsonb->'issue'->>'created_at')::timestamptz as created_at,
    (ws.message::jsonb->'issue'->>'updated_at')::timestamptz as updated_at,
    (ws.message::jsonb->'issue'->>'closed_at')::timestamptz as closed_at,
    ws.message::jsonb->'organization'->>'login' as organization_login,
    ws.event_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS event_ts,
    ws.message::jsonb->'issue'->'assignee'->>'login' as assignee_login,
    (ws.message::jsonb->'issue'->>'id') as issue_id,
    (ws.message::jsonb->'issue'->'user'->>'id') as user_id,
    (ws.message::jsonb->'repository'->>'id') as repository_id,
    (ws.message::jsonb->'issue'->'assignee'->>'id') as assignee_id,
    (ws.message::jsonb->'organization'->>'id') as organization_id
from "STG_SOCIAL".webhook_storage ws
where 1=1
		and system = 'GITHUB'
		and ws.event_ts is not null
		and (ws.message::jsonb->>'action') is not null
		and (ws.message::jsonb->'issue'->>'id') is not null
	limit 100
