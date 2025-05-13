import os
import sys
import json
import requests
import logging
import dotenv
import argparse
import yaml


# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # logging.FileHandler("nocodb_export.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# NocoDB API URL and headers
dotenv.load_dotenv()

NOCODB_API_URL = os.getenv("NOCODB_PUBLISH_API_URL")
NOCODB_API_KEY = os.getenv("NOCODB_PUBLISH_API_KEY")
NOCODB_BASE_ID = os.getenv("NOCODB_PUBLISH_BASE_ID")
NOCODB_HEADERS = {
    "xc-token": NOCODB_API_KEY,
    "Content-Type": "application/json"
}

def create_base(base_name):
    """
    Create a new base in NocoDB.
    http://localhost:8080/api/v2/meta/bases/
    """
    url = f"{NOCODB_API_URL}/api/v2/meta/bases"
    data = {
        "title": base_name,
        "description": "Base created by NocoDB Publish",
    }
    return requests.post(url, headers=NOCODB_HEADERS, json=data)


def get_bases_list():
    """
    Get the list of bases in NocoDB.
    http://localhost:8080/api/v2/meta/bases/
    """
    url = f"{NOCODB_API_URL}/api/v2/meta/bases"
    response = requests.get(url, headers=NOCODB_HEADERS)
    if response.status_code == 200:
        return response.json().get("list", [])
    else:
        logging.error(f"Failed to get bases: {response.text}")
        sys.exit(1)


create_base("Test Base")
bases = get_bases_list()
logging.info(f"List of bases: {json.dumps(bases, indent=2)}")