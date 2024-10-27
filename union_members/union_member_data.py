import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from airflow import DAG

@task(task_id = 'execute_sql_for_data_corr')
def execute_sql(sql_code, postgres_conn_id):
    PostgresOperator(
        task_id = 'execute_sql_op',
        postgres_conn_id = postgres_conn_id,
        sql = sql_code,
    )

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
  "group",
  faculty,
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
  string_agg(distinct case when p.name = 'Электронная почта' then value end, ', ') as электронная_почта,
  string_agg(distinct case when p.name = 'Номер телефона' then value end, ', ') as номер_телефона,
  string_agg(distinct case when p.name = 'Имя пользователя VK' then value end, ', ') as имя_пользователя_vk,
  string_agg(distinct case when p.name = 'Город' then value end, ', ') as город,
  string_agg(distinct case when p.name = 'Родной город' then value end, ', ') as родной_город,
  string_agg(distinct case when p.name = 'Место жительства' then value end, ', ') as место_жительства,
  string_agg(distinct case when p.name = 'Имя пользователя GitHub' then value end, ', ') as имя_пользователя_github,
  string_agg(distinct case when p.name = 'Имя пользователя Telegram' then value end, ', ') as имя_пользователя_telegram,
  string_agg(distinct case when p.name = 'Домашний номер телефона' then value end, ', ') as домашний_номер_телефона,
  string_agg(distinct case when p.name = 'Ступень обучения' then value end, ', ') as ступень_обучения,
  string_agg(distinct case when p.name = 'ВУЗ' then value end, ', ') as вуз,
  string_agg(distinct case when p.name = 'Факультет' then value end, ', ') as факультет,
  string_agg(distinct case when p.name = 'Академическая группа' then value end, ', ') as академическая_группа,
  string_agg(distinct case when p.name = 'Должность' then value end, ', ') as должность,
  string_agg(distinct case when p.name = 'Номер студенческого билета' then value end, ', ') as номер_студенческого_билета,
  string_agg(distinct case when p.name = 'Кафедра' then value end, ', ') as кафедра,
  string_agg(distinct case when p.name = 'Форма обучения' then value end, ', ') as форма_обучения,
  string_agg(distinct case when p.name = 'Полное имя' then value end, ', ') as полное_имя,
  string_agg(distinct case when p.name = 'Дата рождения' then value end, ', ') as дата_рождения,
  string_agg(distinct case when p.name = 'Фото' then value end, ', ') as фото,
  string_agg(distinct case when p.name = 'Пол' then value end, ', ') as пол,
  string_agg(distinct case when p.name = 'Место работы' then value end, ', ') as место_работы,
  string_agg(distinct case when p.name = 'Расположение работы' then value end, ', ') as расположение_работы
from "STG_USERDATA".info i
left join "STG_USERDATA".param p on i.param_id = p.id 
group by owner_id
order by owner_id;
"""

with DAG(
    dag_id = 'data_format_correction',
    start_date = datetime(2024, 1, 1),
    schedule_interval = '@daily',
    catchup=False,
    tags=["dwh", "infra", "union_member", "userdata"],
    description='union_members_data_format_correction',
    default_args = {
        'retries': 1,
        'tags':["dwh", "infra", "union_member", "userdata"],
        'owner':'redstoneenjoyer',
    },
) as dag:
    result = execute_sql(sql_schema, "postgres_dwh")
    # Поменять "postgres_dwh" на aiflow postgress connection id из Admin -> Connections
