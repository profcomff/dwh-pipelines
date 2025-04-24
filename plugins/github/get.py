import logging
from collections.abc import MutableMapping

import pandas as pd
import requests as r

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
