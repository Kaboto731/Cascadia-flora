# test/check_gbif_auth.py
import os

import requests
from dotenv import load_dotenv

load_dotenv()  # pygbif reads environment variables but won't load .env itself

user, pwd = os.environ["GBIF_USER"], os.environ["GBIF_PWD"]
resp = requests.get("https://api.gbif.org/v1/user/login", auth=(user, pwd), timeout=30)
print(resp.status_code)  # 200 means the login works; 401 means wrong credentials
