import json
import re

import requests
from bs4 import BeautifulSoup

import password

# ---------- Get tokens ----------
response = requests.get("https://lk.msuprof.com/adminlogin/?next=/admin")

string = response.headers['Set-Cookie']
pattern = r'csrftoken=\S*;'
string = re.findall(pattern, string)[0]
csrftoken = string.split(sep="=")[1][:-1]

html = BeautifulSoup(response.text, "html.parser")
middle_token = html.find_all("input", {"name": "csrfmiddlewaretoken"})[0].attrs["value"]

# ---------- Login ----------
data = {"username": password.login, "password": password.password,
        "next": "/admin", "csrfmiddlewaretoken": middle_token}
headers = {"cookie": f"csrftoken={csrftoken}", "referer": "https://lk.msuprof.com/adminlogin/?next=/admin"}

response = requests.post("https://lk.msuprof.com/adminlogin/?next=/admin", data=data, headers=headers).request
csrftoken = re.findall(r"csrftoken=(\S*);", response.headers["Cookie"])[0]
sessionid = re.findall(r"sessionid=(\S*)", response.headers["Cookie"])[0]

# ---------- Get Table ----------
headers = {"cookie": f"csrftoken={csrftoken}; sessionid={sessionid}"}
data = {"current-role": "Администратор", "f_role": "", "f_status": "", "page-status": "user"}

response = requests.post("https://lk.msuprof.com/get-table/", data=data, headers=headers)
data = json.loads(response.text)

