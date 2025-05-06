-- truncate old state
delete from "ODS_RENTAL".item_type;

insert into "ODS_RENTAL".item_type(
    uuid,
    name,
    image_url,
    description,
    api_id
)
select
    gen_random_uuid() as uuid,
    name,
    image_url,
    description,
    id
from "STG_RENTAL".item_type
limit 1000001;