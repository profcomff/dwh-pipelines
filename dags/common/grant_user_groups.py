import logging
import typing as tp
from datetime import datetime, timedelta

import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from plugins.api_utils import filter_groups, grant_groups

grant_groups = task(
    task_id="grant_groups",
    retries=3
)(grant_groups)

with DAG(
    dag_id="grant_user_groups",
    start_date=datetime(2024, 11, 30),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["dwh", "infra", "common"],
    default_args={"owner": "mixx3"},
) as dag:
    grant_groups()
