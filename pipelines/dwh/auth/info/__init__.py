import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

from textwrap import dedent
from datetime import datetime
from airflow import DAG


sql_merging_auth = """
upsert into "DWH_AUTH_USER".info 
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
;
"""


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
