from functools import lru_cache

import pandas as pd
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from plugins.github import get_gh_data, get_all_gh_data

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

def fetch_gh_org():
    df = get_organization("profcomff", Variable.get("GITHUB_TOKEN"))
    upload_df(df, "org_info")

def fetch_gh_members():
    """Получить список участников организации"""
    df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/members", Variable.get("GITHUB_TOKEN")
    )
    upload_df(df, "profcomff_member")

def fetch_gh_invations():
    """Получить список приглашений в организацию"""
    df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/invitations",
        Variable.get("GITHUB_TOKEN"),
    )
    upload_df(df, "profcomff_invation")

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
    commits_df["parents"] = commits_df["parents"].apply(
        lambda x: ", ".join(i["sha"] for i in x)
    )
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
    issues_df["labels"] = issues_df["labels"].apply(
        lambda x: ", ".join(i["name"] for i in x)
    )
    issues_df["assignees"] = issues_df["assignees"].apply(
        lambda x: ", ".join(i["login"] for i in x)
    )
    upload_df(issues_df, "profcomff_issue")

def fetch_gh_teams():
    """Получаем информацию об участниках внутри команд"""
    # Получаем команды
    teams_df = get_all_gh_data(
        "https://api.github.com/orgs/profcomff/teams", Variable.get("GITHUB_TOKEN")
    )
    upload_df(teams_df, "profcomff_team")

    # Получаем участников
    members_df = pd.DataFrame()
    for _, (team_id, url) in teams_df[["id", "members_url"]].iterrows():
        url = url.removesuffix("{/member}")
        curr_df = get_all_gh_data(url, Variable.get("GITHUB_TOKEN"))
        curr_df["team_id"] = team_id
        members_df = pd.concat([members_df, curr_df])
    upload_df(members_df, "profcomff_team_member")

    # Получаем репозитории
    repos_df = pd.DataFrame()
    for _, (team_id, url) in teams_df[["id", "repositories_url"]].iterrows():
        curr_df = get_all_gh_data(url, Variable.get("GITHUB_TOKEN"))
        curr_df["team_id"] = team_id
        repos_df = pd.concat([repos_df, curr_df])
    upload_df(repos_df, "profcomff_team_repo")
