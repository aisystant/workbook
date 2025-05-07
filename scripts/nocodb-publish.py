#!/usr/bin/env python

"""
Export data from NocoDB to a Workbook files.
"""

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


def get_workbook_metadata(workbook_path):
    """
    Return content of the metadata.yaml file in the workbook directory.
    """
    metadata_file = os.path.join(workbook_path, "metadata.yaml")
    if not os.path.exists(metadata_file):
        logging.error(f"Metadata file not found: {metadata_file}")
        sys.exit(1)
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)
    if not metadata:
        logging.error(f"Metadata file is empty: {metadata_file}")
        sys.exit(1)
    return metadata


def get_workbook_tables(workbook_path):
    """
    Iterate over all files in the {workbook_path}/tables/*.yaml directory order by filename.
    Return a list of dictionaries with the metadata of each table.
    """
    tables_path = os.path.join(workbook_path, "tables")
    if not os.path.exists(tables_path):
        logging.error(f"Tables directory not found: {tables_path}")
        sys.exit(1)
    tables = []
    for filename in sorted(os.listdir(tables_path)):
        if filename.endswith(".yaml"):
            table_file = os.path.join(tables_path, filename)
            with open(table_file, "r") as f:
                table_metadata = yaml.safe_load(f)
                if table_metadata:
                    tables.append(table_metadata)
    return tables


def nocodb_api_call(method, endpoint, data={}):
    """
    Make a call to the NocoDB API.
    """
    url = f"{NOCODB_API_URL}/{endpoint}"
    headers = NOCODB_HEADERS
    request = requests.request(
        method=method,
        url=url,
        headers=headers,
        json=data
    )
    if request.status_code != 200:
        logging.error(f"Error {request.status_code}: {request.text}")
        raise Exception(f"Error {request.status_code}: {request.text}")
    return request.json()


def get_nocodb_base_schema(base_id):
    """
    Get the NocoDB base with the given ID.

    GET http://localhost:8080/api/v2/meta/bases/{baseId}
    """
    endpoint = f"api/v2/meta/bases/{base_id}"
    response = nocodb_api_call("GET", endpoint)
    return response


def nocodb_create_table(base_id, title, description, columns):
    """
    Create a new table in the NocoDB base.

    POST http://localhost:8080/api/v2/meta/bases/{baseId}/tables
    """
    endpoint = f"api/v2/meta/bases/{base_id}/tables"
    data = {
        "table_name": title,
        "title": title,
        "description": description,
        "columns": columns
    }
    response = nocodb_api_call("POST", endpoint, data)
    return response


def publish_table_to_nocodb(base_id, table_metadata):
    logging.info(f"Publishing table: {table['title']}")
    nocodb_create_table(
        base_id,
        title=table_metadata["title"],
        description=table_metadata.get("description", ""),
        columns=table_metadata["columns"]
    )
    logging.info(f"Table {table['title']} published successfully.")
    return


# Run the script with the following command:
# python scripts/nocodb-publish.py --base <path-to-workbook>

parser = argparse.ArgumentParser(description="Publish a workbook to NocoDB.")
parser.add_argument(
    "--workbook",
    type=str,
    required=True,
    help="Path to the workbook directory to publish."
)
parser.add_argument(
    "--base",
    type=str,
    required=True,
    help="Base ID to publish the workbook to."
)
args = parser.parse_args()

workbook_path = args.workbook
base_id = args.base

base_schema = get_nocodb_base_schema(base_id)
if not base_schema:
    logging.error(f"Base with ID {base_id} not found.")
    sys.exit(1)

logging.info(f"Publishing workbook: {workbook_path}")

#metadata = get_workbook_metadata(workbook_path)
#logging.info(f"Workbook metadata: {metadata}")
tables = get_workbook_tables(workbook_path)
#logging.info(f"Workbook tables: {tables}")

for table in tables:
    publish_table_to_nocodb(base_id, table)
    # Uncomment the following line to stop after the first table
    break