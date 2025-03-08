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
  string_agg(distinct case when i.param_id = 1 then value end, ', ') as email,
  string_agg(distinct case when i.param_id = 2 then value end, ', ') as phone_number,
  string_agg(distinct case when i.param_id = 3 then value end, ', ') as vk_name,
  string_agg(distinct case when i.param_id = 4 then value end, ', ') as city,
  string_agg(distinct case when i.param_id = 5 then value end, ', ') as hometown,
  string_agg(distinct case when i.param_id = 6 then value end, ', ') as location,
  string_agg(distinct case when i.param_id = 7 then value end, ', ') as github_name,
  string_agg(distinct case when i.param_id = 8 then value end, ', ') as telegram_name,
  string_agg(distinct case when i.param_id = 9 then value end, ', ') as home_phone_number,
  string_agg(distinct case when i.param_id = 10 then value end, ', ') as education_level,
  string_agg(distinct case when i.param_id = 11 then value end, ', ') as university,
  string_agg(distinct case when i.param_id = 12 then value end, ', ') as faculty,
  string_agg(distinct case when i.param_id = 13 then value end, ', ') as "group",
  string_agg(distinct case when i.param_id = 14 then value end, ', ') as position,
  string_agg(distinct case when i.param_id = 15 then value end, ', ') as student_id_number,
  string_agg(distinct case when i.param_id = 16 then value end, ', ') as department,
  string_agg(distinct case when i.param_id = 17 then value end, ', ') as mode_of_study,
  string_agg(distinct case when i.param_id = 18 then value end, ', ') as full_name,
  string_agg(distinct case when i.param_id = 19 then value end, ', ') as birth_date,
  string_agg(distinct case when i.param_id = 20 then value end, ', ') as photo,
  string_agg(distinct case when i.param_id = 21 then value end, ', ') as sex,
  string_agg(distinct case when i.param_id = 22 then value end, ', ') as job,
  string_agg(distinct case when i.param_id = 23 then value end, ', ') as work_location
from "STG_USERDATA".info i
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

with DAG(
    dag_id = 'DWH_USER_INFO.info',
    start_date = datetime(2024, 10, 1),
    schedule=[Dataset("STG_USERDATA.info"), Dataset("STG_USERDATA.param")],
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
        inlets = [Dataset("STG_USERDATA.info"), Dataset("STG_USERDATA.param")],
        outlets = [Dataset("DWH_USER_INFO.info")],
    )
