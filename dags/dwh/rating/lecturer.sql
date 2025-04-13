-- close records
        update "DWH_RATING".lecturer as lecturer
        set valid_to_dt = '{{ ds }}'::Date
        where lecturer.api_id NOT IN(
            select dwh.api_id from
                (select
                    api_id,
                    first_name,
                    last_name,
                    middle_name,
                    subject,
                    avatar_link,
                    timetable_id
                from "DWH_RATING".lecturer
                ) as dwh
            join "ODS_RATING".lecturer as ods
            on  dwh.api_id = ods.api_id
            and dwh.first_name = ods.first_name
            and dwh.last_name = ods.last_name
            and dwh.middle_name = ods.middle_name
            and dwh.subject is not distinct from ods.subject
            and dwh.avatar_link = ods.avatar_link
            and dwh.timetable_id = ods.timetable_id
        );

        --evaluate increment
        insert into "DWH_RATING".lecturer
        select 
            ods.*,
            '{{ ds }}'::Date,
            null
            from "ODS_RATING".lecturer as ods
            full outer join "DWH_RATING".lecturer as dwh
              on ods.api_id = dwh.api_id
            where 
              dwh.api_id is NULL
              or dwh.valid_to_dt='{{ ds }}'::Date
        LIMIT 1000000; -- чтобы не раздуло