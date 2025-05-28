delete from "ODS_SOCIAL".viribus_chat;
INSERT INTO "ODS_SOCIAL".viribus_chat
SELECT 
    gen_random_uuid() as uuid,
	coalesce(
		case
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '22151' then 'Frontend'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '55106' then 'Chatting'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '28190' then 'Backend'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '27727' then 'Memes'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '23582' then 'Events'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '81388' then 'AI'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '22152' then 'Chat bots'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '95366' then 'Tvoyff app'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '58057' then 'OS'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '93271' then 'Networks'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '25512' then 'Jobs'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '22155' then 'DevOps'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '24136' then 'Bare metal &IRL'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '58058' then 'Management'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '22303' then 'UI/UX'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '93893' then 'Newbie questions'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '48649' then 'Newbie questions'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '78001' then 'Data science'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '78001' then 'Library'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '36534' then 'Games'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '22810' then 'Project ideas'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '34232' then 'Botalka'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '74434' then 'Random cacao'
			when message::jsonb->'message'->'reply_to_message'->>'message_thread_id' = '36534' then 'Games'
		end
	, 'Service chat') as thread_name,
	message::jsonb->'message'->>'text' as message_text,
	message::jsonb->'message'->'from'->>'username' as sender_telegram_login,
	event_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS message_ts
 
from "STG_SOCIAL".webhook_storage 
where 
	1=1
	and system = 'TELEGRAM'
	and message::jsonb->'message'->'chat'->>'title' ilike '%Viribus Unitis%';
