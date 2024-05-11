import logging
from collections.abc import MutableMapping
from datetime import datetime
from functools import lru_cache

import pandas as pd
import requests as r
import sqlalchemy as sa
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine


def flatten(dictionary, parent_key="", separator="_"):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def get_gh_data(url, token, page, per_page):
    params = {"per_page": per_page, "page": page}
    logging.info(f"Request to {url}, params {params}")
    resp = r.get(
        url,
        params,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    df = pd.DataFrame(flatten(i) for i in resp.json())
    return df


def get_all_gh_data(url, token):
    page = 1
    per_page = 50
    df = get_gh_data(url, token, page, per_page)
    if len(df) == per_page:
        page += 1
        df = pd.concat([df, get_gh_data(url, token, page, per_page)])
    return df


def get_organization(org, token):
    resp = r.get(
        f"https://api.github.com/orgs/{org}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    data = flatten(resp.json())
    df = pd.Series(data).to_frame().T
    return df


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
    df.to_sql(
        table_name, get_conn(), schema="STG_GITHUB", if_exists="append", index=False
    )


@task(task_id="fetch_gh_org", outlets=Dataset("STG_GITHUB.org_info"))
def fetch_gh_org():
    df = get_organization("profcomff", Variable.get("GITHUB_TOKEN"))
    upload_df(df, "org_info")


@task(task_id="fetch_gh_members", outlets=Dataset("STG_GITHUB.profcomff_member"))
def fetch_gh_members():
    """Получить список участников организации"""
    df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/members", Variable.get("GITHUB_TOKEN")
    )
    upload_df(df, "profcomff_member")


@task(task_id="fetch_gh_invations", outlets=Dataset("STG_GITHUB.profcomff_invation"))
def fetch_gh_invations():
    """Получить список приглашений в организацию"""
    df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/invitations",
        Variable.get("GITHUB_TOKEN"),
    )
    upload_df(df, "profcomff_invation")


@task(task_id="fetch_gh_repos", outlets=Dataset("STG_GITHUB.profcomff_repo"))
def fetch_gh_repos():
    """Получить данные из репозиториев в организации"""
    # Получаем репозитории
    repos_df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/repos", Variable.get("GITHUB_TOKEN")
    )
    upload_df(repos_df, "profcomff_repo")

    # Получаем коммиты
    commits_df = pd.DataFrame()
    for _, (repo_id, url) in repos_df[["id", "commits_url"]].iterrows():
        url = url.removesuffix("{/sha}")
        curr_df = get_all_gh_data(url, Variable.get("GITHUB_TOKEN"))
        curr_df["repo_id"] = repo_id
        commits_df = pd.concat([commits_df, curr_df])
    commits_df['parents'] = commits_df['parents'].apply(lambda x: ', '.join(i['sha'] for i in x))
    upload_df(commits_df, "profcomff_commit")

    # Получаем ишьюсы
    issues_df = pd.DataFrame()
    for _, (repo_id, url) in repos_df[["id", "issues_url"]].iterrows():
        url = url.removesuffix("{/number}")
        curr_df = get_all_gh_data(url, Variable.get("GITHUB_TOKEN"))
        curr_df["repo_id"] = repo_id
        issues_df = pd.concat([issues_df, curr_df])
    issues_df.rename(
        columns={
            "reactions_+1": "reactions_like",
            "reactions_-1": "reactions_dislike",
        },
        inplace=True,
    )
    issues_df['labels'] = issues_df['labels'].apply(lambda x: ', '.join(i['name'] for i in x))
    issues_df['assignees'] = issues_df['assignees'].apply(lambda x: ', '.join(i['login'] for i in x))
    upload_df(issues_df, "profcomff_issue")


@task(
    task_id="fetch_gh_teams",
    outlets=[
        Dataset("STG_GITHUB.profcomff_team"),
        Dataset("STG_GITHUB.profcomff_team_member"),
        Dataset("STG_GITHUB.profcomff_team_repo"),
    ],
)
def fetch_gh_teams():
    """Получаем информацию об участниках внутри команд"""
    # Получаем команды
    teams_df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/teams", Variable.get("GITHUB_TOKEN")
    )

    # Получаем участников
    members_df = pd.DataFrame()
    for _, (team_id, url) in teams_df[["id", "members_url"]].iterrows():
        url = url.removesuffix("{/member}")
        curr_df = get_all_gh_data(url, Variable.get("GITHUB_TOKEN"))
        curr_df["team_id"] = team_id
        members_df = pd.concat([members_df, curr_df])

    # Получаем репозитории
    repos_df = pd.DataFrame()
    for i in teams_df["repositories_url"]:
        curr_df = get_all_gh_data(url, Variable.get("GITHUB_TOKEN"))
        curr_df["team_id"] = team_id
        repos_df = pd.concat([repos_df, curr_df])

    upload_df(teams_df, "profcomff_team")
    upload_df(members_df, "profcomff_team_member")
    upload_df(repos_df, "profcomff_team_repo")


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
