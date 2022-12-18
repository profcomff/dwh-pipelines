import requests
from bs4 import BeautifulSoup

response = requests.get("https://lk.msuprof.com/adminlogin/?next=/admin")

html = BeautifulSoup(response.text, "html.parser")
middle_token = html.find_all("input", {"name": "csrfmiddlewaretoken"})[0].attrs["value"]
