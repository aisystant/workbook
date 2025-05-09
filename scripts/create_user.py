import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("NOCODB_PUBLISH_API_URL")
API_TOKEN = os.getenv("NOCODB_PUBLISH_API_KEY")
HEADERS = {"xc-token": API_TOKEN}


url = f"{API_URL}/api/v2/meta/bases/pq75syfa7h2e116/users"
requests.post(url, headers=HEADERS, json={
    "email": "kkkkkkk@dsgadfgsdfgdfsg.com",
    "roles": "owner"
    })

