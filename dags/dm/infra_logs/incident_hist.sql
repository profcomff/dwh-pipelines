with sq as (
    select
        record->>'message' as e_msg,
        container_name,
        create_ts
    from "ODS_INFRA_LOGS".container_log
    where
        record->>'level_name' = 'ERROR' or record->>'level_name' = 'CRITICAL') 
merge into "DM_INFRA_LOGS".incident_hint as ne 
using sq as e 
    on (ne.container_name = e.container_name) and (ne.message = e.e_msg) and (ne.create_ts = e.create_ts)
when not matched then 
    insert (id,msk_record_loaded_dttm,container_name,message,create_ts)
    values (DEFAULT,now(),e.container_name,e.e_msg,e.create_ts)
