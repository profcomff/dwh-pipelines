-- calculate rank
with ranked as (
    select api_id, ROW_NUMBER() OVER (ORDER BY mark_weighted DESC) as rank 
    from "DWH_RATING".lecturer
    where valid_to_dt is null
)
update "DWH_RATING".lecturer as l
set rank = ranked.rank
from ranked
where l.api_id = ranked.api_id
and l.valid_to_dt is null;
