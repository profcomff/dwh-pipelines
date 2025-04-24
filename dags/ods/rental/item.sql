-- truncate old states
delete from "ODS_RENTAL".item;

insert into "ODS_RENTAL".item(
    uuid,
    api_id,
    type_id,
    is_available,
    type
)
select 
    gen_random_uuid() as uuid,
    api_id,
    type_id,
    is_available,
    type
from "STG_RENTAL".item
limit 1000001;