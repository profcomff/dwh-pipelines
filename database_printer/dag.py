import requests
from bs4 import BeautifulSoup

response = requests.get("https://lk.msuprof.com/adminlogin/?next=/admin")

string = response.headers['Set-Cookie']
pattern = r'csrftoken=\S*;'
string = re.findall(pattern, string)[0]
csrftoken = string.split(sep = "=")[1][:-1]

html = BeautifulSoup(response.text, "html.parser")
middle_token = html.find_all("input", {"name": "csrfmiddlewaretoken"})[0].attrs["value"]
