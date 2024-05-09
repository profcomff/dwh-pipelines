from functools import lru_cache
import logging
from datetime import datetime, timedelta
from collections.abc import MutableMapping

import pandas as pd
from sqlalchemy import create_engine
import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable


def flatten(dictionary, parent_key='', separator='_'):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def get_organization(org, token):
    resp = r.get(
        f'https://api.github.com/orgs/{org}',
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    data = flatten(resp.json())
    df = pd.Series(data).to_frame().T
    return df


def get_members(org, token):
    """Получить список участников организации"""
    resp = r.get(
        f'https://api.github.com/orgs/{org}/members',
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    df = pd.DataFrame(resp.json())
    return df


def get_invations(org, token):
    """Получить список приглашений в организацию"""
    resp = r.get(
        f'https://api.github.com/orgs/{org}/invitations',
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    df = pd.DataFrame(flatten(i) for i in resp.json())
    return df


def get_repos(org, token):
    resp = r.get(
        f'https://api.github.com/users/{org}/repos',
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    df = pd.DataFrame(flatten(i) for i in resp.json())
    return df


def get_teams(org, token):
    resp = r.get(
        f'https://api.github.com/orgs/{org}/teams',
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    teams_df = pd.DataFrame(resp.json())

    members_df = pd.DataFrame()
    for i in resp.json():
        respmembers = r.get(
            i["members_url"].removesuffix('{/member}'),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        df = pd.DataFrame(respmembers.json())
        df['team_id'] = i["id"]
        members_df = pd.concat([members_df, df])

    repos_df = pd.DataFrame()
    for i in resp.json():
        resprepos = r.get(
            i["repositories_url"],
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        df = pd.DataFrame(flatten(i) for i in resprepos.json())
        df['team_id'] = i["id"]
        repos_df = pd.concat([repos_df, df])

    return teams_df, members_df, repos_df


@lru_cache()
def get_conn():
    conn = (
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
        .replace("?__extra__=%7B%7D", "")
    )
    engine = create_engine(conn)
    return engine


def upload_df(df: pd.DataFrame, table_name):
    get_conn().execute(f'TRUNCATE TABLE "STG_GITHUB".{table_name};')
    df.to_sql(table_name, get_conn(), schema="STG_GITHUB", if_exists="append", index=False)


@task(task_id="fetch_gh_org", outlets=Dataset("STG_GITHUB.org_info"))
def fetch_gh_org():
    df = get_organization('profcomff', Variable.get("GITHUB_TOKEN"))
    upload_df(df, 'org_info')

@task(task_id="fetch_gh_members", outlets=Dataset("STG_GITHUB.profcomff_member"))
def fetch_gh_members():
    df = get_members('profcomff', Variable.get("GITHUB_TOKEN"))
    upload_df(df, 'profcomff_member')

@task(task_id="fetch_gh_invations", outlets=Dataset("STG_GITHUB.profcomff_invation"))
def fetch_gh_invations():
    df = get_invations('profcomff', Variable.get("GITHUB_TOKEN"))
    upload_df(df, 'profcomff_invation')

@task(task_id="fetch_gh_repos", outlets=Dataset("STG_GITHUB.profcomff_repo"))
def fetch_gh_repos():
    df = get_repos('profcomff', Variable.get("GITHUB_TOKEN"))
    upload_df(df, 'profcomff_repo')

@task(task_id="fetch_gh_teams", outlets=[Dataset("STG_GITHUB.profcomff_team"), Dataset("STG_GITHUB.profcomff_team_member"), Dataset("STG_GITHUB.profcomff_team_repo")])
def fetch_gh_teams():
    teams_df, members_df, repos_df = get_teams('profcomff', Variable.get("GITHUB_TOKEN"))
    upload_df(teams_df, 'profcomff_team')
    upload_df(members_df, 'profcomff_team_member')
    upload_df(repos_df, 'profcomff_team_repo')


with DAG(
    dag_id="github_get_organization",
    start_date=datetime(2022, 1, 1),
    schedule="0 0 */1 * *",
    catchup=False,
    tags= ["dwh", "infra", "github"],
    default_args={"owner": "dyakovri"}
) as dag:
    fetch_gh_org() >> fetch_gh_members() >> fetch_gh_invations() >> fetch_gh_repos() >> fetch_gh_teams()
