with new_log as (
    select
        coalesce (container_name, 'all') as container_name,
        coalesce(to_char(date_trunc('day', create_ts), 'YYYY-MM-DD'), 'all') as create_date,
        coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='DEBUG' then 1 else 0 end), 0) as debug_cnt,
        coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='WARNING' then 1 else 0 end), 0) as warn_cnt,
        coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='INFO' then 1 else 0 end), 0) as info_cnt,
        coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='ERROR' then 1 else 0 end), 0) as error_cnt,
        coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}'))='CRITICAL' then 1 else 0 end), 0) as critical_cnt,
        coalesce(sum(case when upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}')) <> ALL ('{"DEBUG", "WARNING", "INFO", "ERROR", "CRITICAL"}') then 1 else 0 end), 0) as other_cnt
    from "ODS_INFRA_LOGS".container_log
    where
        container_name like 'com_profcomff_api_%'
        and upper(coalesce(record #>> '{level_name}', record #>> '{loglevel}')) is not null
    group by cube(container_name, date_trunc('day', create_ts))
)
merge into "DM_INFRA_LOGS".container_log_cube as clc
using new_log as nl
    on nl.container_name = clc.container_name and nl.create_date = clc.create_date
when matched and not (nl.container_name = 'all') and not(nl.create_date = 'all') then
    update set
        debug_cnt = greatest(clc.debug_cnt, nl.debug_cnt),
        warn_cnt = greatest(clc.warn_cnt, nl.warn_cnt),
        info_cnt = greatest(clc.info_cnt, nl.info_cnt),
        error_cnt = greatest(clc.error_cnt, nl.error_cnt),
        critical_cnt = greatest(clc.critical_cnt, nl.critical_cnt),
        other_cnt = greatest(clc.other_cnt, nl.other_cnt)
when not matched and not (nl.container_name = 'all' and nl.create_date = 'all') then
    insert (container_name, create_date, debug_cnt, warn_cnt, info_cnt, error_cnt, critical_cnt, other_cnt)
    values (nl.container_name, nl.create_date, nl.debug_cnt, nl.warn_cnt, nl.info_cnt, nl.error_cnt, nl.critical_cnt, nl.other_cnt);
