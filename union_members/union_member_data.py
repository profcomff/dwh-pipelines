import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG

sql_sorting_for_DWH = """
insert into "DWH_USER_INFO".info (
  user_id,
  email,
  phone_number,
  vk_name,
  city,
  hometown,
  location,
  github_name,
  telegram_name,
  home_phone_number,
  education_level,
  university,
  faculty,
  "group",
  position,
  student_id_number,
  department,
  mode_of_study,
  full_name,
  birth_date,
  photo,
  sex,
  job,
  work_location
)
select -- полная таблица
  owner_id as user_id,
  string_agg(distinct case when p.name = 'Электронная почта' then value end, ', ') as email,
  string_agg(distinct case when p.name = 'Номер телефона' then value end, ', ') as phone_number,
  string_agg(distinct case when p.name = 'Имя пользователя VK' then value end, ', ') as vk_name,
  string_agg(distinct case when p.name = 'Город' then value end, ', ') as city,
  string_agg(distinct case when p.name = 'Родной город' then value end, ', ') as hometown,
  string_agg(distinct case when p.name = 'Место жительства' then value end, ', ') as location,
  string_agg(distinct case when p.name = 'Имя пользователя GitHub' then value end, ', ') as github_name,
  string_agg(distinct case when p.name = 'Имя пользователя Telegram' then value end, ', ') as telegram_name,
  string_agg(distinct case when p.name = 'Домашний номер телефона' then value end, ', ') as home_phone_number,
  string_agg(distinct case when p.name = 'Ступень обучения' then value end, ', ') as education_level,
  string_agg(distinct case when p.name = 'ВУЗ' then value end, ', ') as university,
  string_agg(distinct case when p.name = 'Факультет' then value end, ', ') as faculty,
  string_agg(distinct case when p.name = 'Академическая группа' then value end, ', ') as "group",
  string_agg(distinct case when p.name = 'Должность' then value end, ', ') as position,
  string_agg(distinct case when p.name = 'Номер студенческого билета' then value end, ', ') as student_id_number,
  string_agg(distinct case when p.name = 'Кафедра' then value end, ', ') as department,
  string_agg(distinct case when p.name = 'Форма обучения' then value end, ', ') as mode_of_study,
  string_agg(distinct case when p.name = 'Полное имя' then value end, ', ') as full_name,
  string_agg(distinct case when p.name = 'Дата рождения' then value end, ', ') as birth_date,
  string_agg(distinct case when p.name = 'Фото' then value end, ', ') as photo,
  string_agg(distinct case when p.name = 'Пол' then value end, ', ') as sex,
  string_agg(distinct case when p.name = 'Место работы' then value end, ', ') as job,
  string_agg(distinct case when p.name = 'Расположение работы' then value end, ', ') as work_location
from "STG_USERDATA".info i
left join "STG_USERDATA".param p on i.param_id = p.id 
group by owner_id
on conflict (user_id) do update set
	email = EXCLUDED.email,
	phone_number = EXCLUDED.phone_number,
	vk_name = EXCLUDED.vk_name,
	city = EXCLUDED.city,
	hometown = EXCLUDED.hometown,
	location = EXCLUDED.location,
	github_name = EXCLUDED.github_name,
	telegram_name = EXCLUDED.telegram_name,
	home_phone_number = EXCLUDED.home_phone_number,
	education_level = EXCLUDED.education_level,
	university = EXCLUDED.university,
	faculty = EXCLUDED.faculty,
	"group" = EXCLUDED."group",
	position = EXCLUDED.position,
	student_id_number = EXCLUDED.student_id_number,
	department = EXCLUDED.department,
	mode_of_study = EXCLUDED.mode_of_study,
	full_name = EXCLUDED.full_name,
	birth_date = EXCLUDED.birth_date,
	photo = EXCLUDED.photo,
	sex = EXCLUDED.sex,
	job = EXCLUDED.job,
	work_location = EXCLUDED.work_location;
"""

sql_merging_auth = """
insert into "DWH_AUTH_USER".info 
(
  id,
  email,
  auth_email,
  phone_number,
  vk_name,
  city,
  hometown,
  location,
  github_name,
  telegram_name,
  home_phone_number,
  education_level,
  university,
  faculty,
  "group",
  position,
  student_id_number,
  department,
  mode_of_study,
  full_name,
  birth_date,
  photo,
  sex,
  job,
  work_location,
  is_deleted
)
select
  user_id as id,
  email,
  auth_email,
  phone_number,
  vk_name,
  city,
  hometown,
  location,
  github_name,
  telegram_name,
  home_phone_number,
  education_level,
  university,
  faculty,
  "group",
  position,
  student_id_number,
  department,
  mode_of_study,
  full_name,
  birth_date,
  photo,
  sex,
  job,
  work_location,
  is_deleted
from "DWH_USER_INFO".info
left join
(
select
    user_id,
    any_value(u.is_deleted) as is_deleted,
    string_agg(distinct case when param = 'email' then value end, ', ') as auth_email
from "STG_AUTH".auth_method as au
left join "STG_AUTH".user as u on u.id = au.user_id
group by au.user_id
) using (user_id)
on conflict (id) do update set
	email = EXCLUDED.email,
	auth_email = EXCLUDED.auth_email,
	phone_number = EXCLUDED.phone_number,
	vk_name = EXCLUDED.vk_name,
	city = EXCLUDED.city,
	hometown = EXCLUDED.hometown,
	location = EXCLUDED.location,
	github_name = EXCLUDED.github_name,
	telegram_name = EXCLUDED.telegram_name,
	home_phone_number = EXCLUDED.home_phone_number,
	education_level = EXCLUDED.education_level,
	university = EXCLUDED.university,
	faculty = EXCLUDED.faculty,
	"group" = EXCLUDED."group",
	position = EXCLUDED.position,
	student_id_number = EXCLUDED.student_id_number,
	department = EXCLUDED.department,
	mode_of_study = EXCLUDED.mode_of_study,
	full_name = EXCLUDED.full_name,
	birth_date = EXCLUDED.birth_date,
	photo = EXCLUDED.photo,
	sex = EXCLUDED.sex,
	job = EXCLUDED.job,
	work_location = EXCLUDED.work_location,
	is_deleted = EXCLUDED.is_deleted;
"""

with DAG(
    dag_id = 'DWH_USER_INFO.info',
    start_date = datetime(2024, 10, 1),
    schedule=[Dataset("ODS_INFO.info_hist"), Dataset("ODS_INFO.param_hist")],
    catchup=False,
    tags=["dwh", "core", "user_info"],
    description='union_members_data_format_correction',
    default_args = {
        'retries': 1,
        'owner':'redstoneenjoyer',
    },
):
    PostgresOperator(
        task_id='execute_sql_for_data_corr',
        postgres_conn_id="postgres_dwh",
        sql=dedent(sql_sorting_for_DWH),
        inlets = [Dataset("ODS_INFO.param_hist"), Dataset("ODS_INFO.info_hist")],
        outlets = [Dataset("DWH_USER_INFO.info")],
    )


with DAG(
    dag_id = 'DWH_AUTH_USER.info',
    start_date = datetime(2024, 10, 1),
    schedule=[Dataset("DWH_USER_INFO.info"), Dataset("ODS_AUTH.auth_method"), Dataset("ODS_AUTH.user")],
    catchup=False,
    tags=["dwh", "src", "user_info"],
    description='union_members_data_format_correction',
    default_args = {
        'retries': 1,
        'owner':'redstoneenjoyer',
    },
):
    PostgresOperator(
        task_id='merginng_and_inserting_into_ODS_INFO',
        postgres_conn_id="postgres_dwh",
        sql=dedent(sql_merging_auth),
        inlets = [Dataset("DWH_USER_INFO.info"), Dataset("ODS_AUTH.auth_method"), Dataset("ODS_AUTH.user")],
        outlets = [Dataset("DWH_AUTH_USER.info")],
    )


with DAG(
    dag_id = 'ODS_INFO.info_hist',
    start_date = datetime(2024, 11, 1),
    schedule=[Dataset("STG_USERDATA.info")],
    catchup=False,
    tags=["ods", "src", "userdata"],
    description='scd2_info_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='info_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        update "ODS_INFO".info_hist as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id NOT IN(
            select ods.id from
                (select 
                    id,
                    param_id,
                    source_id,
                    owner_id,
                    value,
                    create_ts,
                    modify_ts,
                    is_deleted
                from "ODS_INFO".info_hist
                ) as ods
            join "STG_USERDATA".info as stg
            on ods.id = stg.id
            and ods.param_id = stg.param_id
            and ods.source_id = stg.source_id
            and ods.owner_id = stg.owner_id
            and ods.value = stg.value
            and ods.create_ts = stg.create_ts
            and ods.modify_ts = stg.modify_ts
            and ods.is_deleted = stg.is_deleted
        );

        --evaluate increment
        insert into "ODS_INFO".info_hist
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_USERDATA".info as stg
            full outer join "ODS_INFO".info_hist as ods
              on stg.id = ods.id
            where 
              ods.id is NULL
              or ods.valid_to_dt='{{ ds }}'::Date
        LIMIT 100000; -- чтобы не раздуло
        """),
        inlets = [Dataset("STG_USERDATA.info")],
        outlets = [Dataset("ODS_INFO.info_hist")],
    )


with DAG(
    dag_id = 'ODS_INFO.param_hist',
    start_date = datetime(2024, 11, 1),
    schedule=[Dataset("STG_USERDATA.param")],
    catchup=False,
    tags=["ods", "src", "userdata"],
    description='scd2_info_hist',
    default_args = {
        'retries': 1,
        'owner':'mixx3',
    },
):
    PostgresOperator(
        task_id='param_hist',
        postgres_conn_id="postgres_dwh",
        sql=dedent("""
        update "ODS_INFO".param_hist as am
        set valid_to_dt = '{{ ds }}'::Date
        where am.id NOT IN(
            select ods.id from
                (select 
                    id,
                    name,
                    category_id,
                    is_required,
                    changeable,
                    type,
                    create_ts,
                    modify_ts,
                    is_deleted,
                    validation
                from "ODS_INFO".param_hist
                ) as ods
            join "STG_USERDATA".param as stg
            on ods.id = stg.id
            and ods.name = stg.name
            and ods.category_id = stg.category_id
            and ods.is_required = stg.is_required
            and ods.changeable = stg.changeable
            and ods.type = stg.type
            and ods.create_ts = stg.create_ts
            and ods.modify_ts = stg.modify_ts
            and ods.is_deleted = stg.is_deleted
        );

        --evaluate increment
        insert into "ODS_INFO".param_hist
        select 
            stg.*,
            '{{ ds }}'::Date,
            null
            from "STG_USERDATA".param as stg
            full outer join "ODS_INFO".param_hist as ods
              on stg.id = ods.id
            where 
              ods.id is NULL
              or ods.valid_to_dt='{{ ds }}'::Date
        LIMIT 100000; -- чтобы не раздуло
        """),
        inlets = [Dataset("STG_USERDATA.param")],
        outlets = [Dataset("ODS_INFO.param_hist")],
    )
