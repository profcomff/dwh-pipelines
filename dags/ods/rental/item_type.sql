-- truncate old state
delete from "ODS_RENTAL".item_type;

insert into "ODS_RENTAL".item_type(
    uuid,
    api_id,
    name,
    image_url,
    description
)
select
    gen_random_uuid() as uuid,
    api_id,
    name,
    image_url,
    description
from "STG_RENTAL".item_type
limit 1000001;