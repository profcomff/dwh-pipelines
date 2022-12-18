import re

import requests
from bs4 import BeautifulSoup

import password

# ---------- Get tokens ----------
response = requests.get("https://lk.msuprof.com/adminlogin/")

string = response.headers['Set-Cookie']
pattern = r'csrftoken=\S*;'
string = re.findall(pattern, string)[0]
csrftoken = string.split(sep="=")[1][:-1]

html = BeautifulSoup(response.text, "html.parser")
middle_token = html.find_all("input", {"name": "csrfmiddlewaretoken"})[0].attrs["value"]

# ---------- Login ----------
json = {"username": password.login, "password": password.password,
        "next": "/admin", "csrfmiddlewaretoken": middle_token}
cookies = {"csrftoken": csrftoken}
headers = {"referer": "https://lk.msuprof.com/adminlogin/?next=/admin"}

response = requests.post("https://lk.msuprof.com/adminlogin/", json=json, cookies=cookies, headers=headers)
response.encoding = "utf-8"

file = open("test.html", "w", encoding="utf-8")
file.write(response.text)

print(response.headers)
