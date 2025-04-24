import logging
from datetime import datetime
from functools import lru_cache

import pandas as pd
import requests as r
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from plugins.github import (fetch_gh_invations, fetch_gh_members, fetch_gh_org,
                            fetch_gh_repos, fetch_gh_teams, flatten,
                            get_all_gh_data, get_conn, get_gh_data,
                            get_organization, upload_df)

fetch_gh_org = task(task_id="fetch_gh_org", outlets=Dataset("STG_GITHUB.org_info"))(
    fetch_gh_org
)

fetch_gh_members = task(
    task_id="fetch_gh_members", outlets=Dataset("STG_GITHUB.profcomff_member")
)(fetch_gh_members)

fetch_gh_invations = task(
    task_id="fetch_gh_invations", outlets=Dataset("STG_GITHUB.profcomff_invation")
)(fetch_gh_invations)

fetch_gh_repos = task(
    task_id="fetch_gh_repos", outlets=Dataset("STG_GITHUB.profcomff_repo")
)(fetch_gh_repos)

with DAG(
    dag_id="github_get_organization",
    start_date=datetime(2022, 1, 1),
    schedule="0 0 */1 * *",
    catchup=False,
    tags=["dwh", "infra", "github"],
    default_args={"owner": "dyakovri"},
) as dag:
    (
        fetch_gh_org()
        >> fetch_gh_members()
        >> fetch_gh_invations()
        >> fetch_gh_repos()
        >> fetch_gh_teams()
    )
