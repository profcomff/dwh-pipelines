import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG

sql_schema = """
insert into "ODS_INFO".info (
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
(
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
"STG_USERDATA".info i
left join "STG_USERDATA".param p on i.param_id = p.id 
group by owner_id
order by owner_id;
) source_table
on conflict (user_id)
do update set
	email = EXCLUDED.email;
	phone_number = EXCLUDED.phone_number;
	vk_name = EXCLUDED.vk_name;
	city = EXCLUDED.city;
	hometown = EXCLUDED.hometown;
	location = EXCLUDED.location;
	github_name = EXCLUDED.github_name;
	telegram_name = EXCLUDED.telegram_name;
	home_phone_number = EXCLUDED.home_phone_number;
	education_level = EXCLUDED.education_level;
	university = EXCLUDED.university;
	faculty = EXCLUDED.faculty;
	"group" = EXCLUDED."group";
	position = EXCLUDED.position;
	student_id_number = EXCLUDED.student_id_number;
	department = EXCLUDED.department;
	mode_of_study = EXCLUDED.mode_of_study;
	full_name = EXCLUDED.full_name;
	birth_date = EXCLUDED.birth_date;
	photo = EXCLUDED.photo;
	sex = EXCLUDED.sex;
	job = EXCLUDED.job;
	work_location = EXCLUDED.work_location;
"""

with DAG(
    dag_id = 'inserting_into_ODS_INFO.info',
    start_date = datetime(2024, 1, 1),
    schedule_interval = '@daily',
    catchup=False,
    tags=["dwh", "union_member", "userdata"],
    description='union_members_data_format_correction',
    default_args = {
        'retries': 1,
        'tags':["ods", "union_member", "userdata"],
        'owner':'redstoneenjoyer',
    },
):
    PostgresOperator(
        task_id='execute_sql_for_data_corr',
        postgres_conn_id="postgres_dwh",
        sql=dedent(sql_schema),
        inlets = [Dataset("STG_USERDATA")],
        outlets = [Dataset("ODS_INFO")],
    )