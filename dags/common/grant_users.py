import logging
import typing as tp
from datetime import datetime, timedelta

import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

environment = Variable.get("_ENVIRONMENT")

DWH_DB_DSN = (
    Connection.get_connection_from_secrets("supercomnnection")  # TODO@mixx3 fix this!
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)

# exclude list for tables with sensetive data
DENY_ACCESS_TABLE_LIST = Variable.get("DENY_ACCESS_TABLE_LIST").split(",")

# users allowed to read sensetive data
ALLOW_SELECT_USER_LIST = Variable.get("ALLOW_SELECT_USER_LIST").split(",")

# users w all permissions to tables without sensetive data, ex. dwh developers
# TODO@mixx3 parse this data from auth.scopes, support it on backend
ALLOW_ALL_USER_LIST = Variable.get("ALLOW_ALL_USER_LIST").split(",")


def filter_groups(
    groups: tp.List[tp.List[str]], exclude_list: tp.List[str]
) -> tp.Tuple[tp.List[str]]:
    res = []
    excluded = []
    for group in groups:
        to_exclude = False
        for excl in exclude_list:
            if excl in group[0]:
                to_exclude = True
        if to_exclude:
            excluded.append(group[0])
        else:
            res.append(group[0])
    return res, excluded


@task(task_id="grant_groups", retries=3)
def grant_groups():
    dwh_sql_engine = create_engine(DWH_DB_DSN)
    with dwh_sql_engine.connect() as dwh_conn:
        users = dwh_conn.execute(
            sa.text(
                """
                select 
                    usename
                from pg_catalog.pg_user
                where usename not ilike '%srvc%' and not usesuper;
            """
            )
        ).fetchall()
        logging.info(len(users))
        read_only_groups = dwh_conn.execute(
            sa.text(
                """
                select 
                    groname
                from pg_catalog.pg_group
                where groname ilike '%test_dwh%' and groname ilike '%read%';
            """
                if environment == "test"
                else """
                select 
                    groname
                from pg_catalog.pg_group
                where groname ilike '%prod_dwh%' and groname ilike '%read%';
            """
            )
        ).fetchall()
        logging.info(len(users))
        # drop all users from all groups
        groups, excluded = filter_groups(read_only_groups, DENY_ACCESS_TABLE_LIST)
        # add users
        for group_name in groups:
            for user in users:
                try:
                    dwh_conn.execute(
                        sa.text(f"""alter group {group_name} add user {user[0]}""")
                    )
                except Exception as e:
                    logging.warning(
                        f"{user[0]} is already in group {group_name} or something else happened"
                    )
                    logging.error(str(e))

        # sensetive data
        for group_name in excluded:
            for user in ALLOW_SELECT_USER_LIST:
                try:
                    dwh_conn.execute(
                        sa.text(f"""alter group {group_name} add user {user}""")
                    )
                except Exception as e:
                    logging.warning(
                        f"{user[0]} is already in group {group_name} or something else happened"
                    )
                    logging.error(str(e))
        # TODO@mixx3 add scope to dev users


with DAG(
    dag_id="grant_user_groups",
    start_date=datetime(2024, 11, 30),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["dwh", "infra", "common"],
    default_args={"owner": "mixx3"},
) as dag:
    grant_groups()
