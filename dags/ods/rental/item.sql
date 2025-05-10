-- truncate old states
delete from "ODS_RENTAL".item;

insert into "ODS_RENTAL".item(
    uuid,
    api_id,
    type_id,
    is_available
)
select 
    gen_random_uuid() as uuid,
    id,
    type_id,
    is_available
from "STG_RENTAL".item
limit 100000;