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
import time


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
NOCODB_API_URL = os.getenv("NOCODB_EXPORT_API_URL")
NOCODB_API_KEY = os.getenv("NOCODB_EXPORT_API_KEY")

NOCODB_HEADERS = {
    "xc-token": NOCODB_API_KEY,
    "Content-Type": "application/json"
}

USE_MOCKUPS = os.getenv("USE_MOCKUPS", "false").lower() == "true"


def api_call(path, mockup=USE_MOCKUPS):
    """
    Get the mockup file from the mocks directory.
    """
    # check mocks/{path}.json file
    path_without_query = path.split("?")[0]
    fn = f"mocks/{path_without_query}.json"
    if mockup and os.path.exists(fn):
        logging.info(f"Mockup file found: {fn}")
        with open(fn, "r") as f:
            return json.load(f)
    # if not, call the API
    if mockup:
        logging.info(f"Mockup file not found: {fn}")
    url = f"{NOCODB_API_URL}/{path}"
    response = requests.get(url, headers=NOCODB_HEADERS)
    if response.status_code != 200:
        logging.error(f"Failed to get {path}: {response.status_code} - {response.text}")
        raise Exception(f"Failed to get {path}: {response.status_code} - {response.text}")
    if mockup:
        # create the parent directory if it doesn't exist
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        # write the response to the file
        with open(fn, "w") as f:
            json.dump(response.json(), f, indent=4)
            logging.info(f"Mockup file created: {fn}")
    return response.json()
    

def get_base_tables(baseId):
    """
    Get all tables in a base.
    """
    result = api_call(f"api/v2/meta/bases/{baseId}/tables")
    return result.get("list", [])


def get_table_metadata(tableId):
    """
    Get metadata for a table.
    """
    result = api_call(f"api/v2/meta/tables/{tableId}")
    return result


def print_table_debug_info(table):
    # Table keys:  dict_keys(['id', 'source_id', 'base_id', 'table_name', 'title', 'type', 'meta', 'schema', 'enabled', 'mm', 'tags', 'pinned', 'deleted', 'order', 'created_at', 'updated_at', 'description', 'fk_workspace_id', 'synced'])
    print(f"ID: {table['id']}")
    print(f"Title: {table['title']}")
    print(f"Type: {table['type']}")
    print(f"Enabled: {table['enabled']}")
    print(f"Created at: {table['created_at']}")
    print(f"Updated at: {table['updated_at']}")
    print(f"Description: {table['description']}")
    print(f"Tags: {table['tags']}")
    print(f"Schema: {table['schema']}")
    print(f"Meta: {table['meta']}")
    print(f"MM: {table['mm']}")
    print(f"Source ID: {table['source_id']}")
    print(f"Base ID: {table['base_id']}")
    print(f"FK Workspace ID: {table['fk_workspace_id']}")
    print(f"Synced: {table['synced']}")
    print(f"Deleted: {table['deleted']}")
    print(f"Order: {table['order']}")
    print(f"Pinned: {table['pinned']}")
    print(f"Table name: {table['table_name']}")
    print('-----------------------------------')


def build_workbook_columns(column):
    """
    Build a column from a table column.

    Example column:
    ```json
        {
            "id": "cyh9dqjt26aoa0w",
            "source_id": "bqh2kn3vkl3fi7w",
            "base_id": "p9v5nmncfi5qveh",
            "fk_model_id": "m2s52x77r4yxzhc",
            "title": "Id",
            "column_name": "id",
            "uidt": "ID",
            "dt": "int4",
            "np": null,
            "ns": null,
            "clen": null,
            "cop": null,
            "pk": true,
            "pv": null,
            "rqd": true,
            "un": false,
            "ct": null,
            "ai": true,
            "unique": null,
            "cdf": null,
            "cc": null,
            "csn": null,
            "dtx": "specificType",
            "dtxp": "11",
            "dtxs": "",
            "au": null,
            "validate": null,
            "virtual": null,
            "deleted": null,
            "system": null,
            "order": 1,
            "created_at": "2024-11-23 18:50:01+00:00",
            "updated_at": "2024-11-23 18:50:01+00:00",
            "meta": {
                "defaultViewColOrder": 2,
                "defaultViewColVisibility": true
            },
            "description": null,
            "fk_workspace_id": "wzs0rnhh",
            "readonly": false,
            "custom_index_name": null
        },
    ```
    """
    # get the column metadata
    column_metadata = {
        "title": column["title"] or "",
        "description": column["description"] or "",
        "uidt": column["uidt"] or "",
        "pv": column["pv"] or False,
    }
    return column_metadata


def build_workbook_row_tuple(row, columns):
    """
    Build a row tuple from a row data and columns.
    """
    row_id = row["Id"]
    data = []
    for column in columns:
        column_name = column["title"]
        # skip columns that are not in the row data
        if column_name not in row:
            value = ""
        # get the value from the row data
        value = row[column_name]
        if value is None:
            value = ""
        # add the value to the data dictionary
        data.append(value)
    return (row_id, data)


SKIP_COLUMNS = [
    "Id",
    "CreatedAt",
    "nc_created_by",
    "UpdatedAt",
    "nc_updated_by",
    "nc_order",
    "LastModifiedBy",
    "Date",
    "Номер",
]


def get_table_data(tableId):
    """
    Get data for a table.
    GET http://localhost:8080/api/v2/tables/{tableId}/records
    """
    result = api_call(f"api/v2/tables/{tableId}/records?limit=1000")
    return result.get("list", [])


def clean_row_data(row):
    """
    Replace None values with empty strings.
    """
    for key, value in row.items():
        if value is None:
            row[key] = ""
    return row


def build_workbook(tableId):
    """
    Build a workbook from a table.

    Example:

    ```yaml
    title: Introduce Yourself
    description: This is a test workbook for testing purposes
    columns:
    - title: Number
        description: Your number
        uidt: SingleLineText
        pv: yes
    - title: Name
        description: Your name
        uidt: SingleLineText
    rows:
    - "Number": 1
        "Name": John Doe
    ```
    """
    # get the table metadata
    table_metadata = get_table_metadata(tableId)
    # get table data
    try:
        table_data = get_table_data(tableId)
    except Exception as e:
        table_data = []
    # create the workbook
    columns = table_metadata["columns"]
    filtered_columns = [column for column in columns if column["title"] not in SKIP_COLUMNS]
    ordered_columns = sorted(filtered_columns, key=lambda x: x.get("meta", {}).get("defaultViewColOrder"))
    workbook = {
        "title": table_metadata["title"],
        "description": table_metadata["description"],
        "columns": [build_workbook_columns(column) for column in ordered_columns],
        "rows": dict([build_workbook_row_tuple(row, ordered_columns) for row in table_data]),
    }
    return workbook


def get_table_filename(table_metadata):
    """
    Get the filename for a table.

    Example: {000.order}-table.id
    """
    order = int(table_metadata['order'] * 10)
    return f"{order:04d}.{table_metadata['id']}.yaml"
    

def process_table(table):
    """
    Process a table and save it to a file.
    """
    # print_table_debug_info(table)
    table_metadata = get_table_metadata(table["id"])
    # print table metadata keys
    workbook_table = build_workbook(table["id"])
    # print workbook yaml for debugging
    # print(yaml.dump(workbook_table, allow_unicode=True, sort_keys=False))
    # get the filename for the table
    filename = get_table_filename(table_metadata)
    full_path = os.path.join(args.workbook, "tables", filename)
    # create the parent directory if it doesn't exist
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    # write the workbook to the file
    with open(full_path, "w") as f:
        yaml.dump(workbook_table, f, allow_unicode=True, sort_keys=False)
        logging.info(f"Workbook file created: {full_path}")


# Init arg parser
parser = argparse.ArgumentParser(description="Export workbook from NocoDB.")
parser.add_argument(
    "--base",
    type=str,
    required=True,
    help="Base ID to export"
)
parser.add_argument(
    "--workbook",
    type=str,
    required=True,
    help="Path to the workbook directory to publish."
)

args = parser.parse_args()
NOCODB_BASE_ID = args.base

bases = get_base_tables(NOCODB_BASE_ID)
for table in bases:
    try:
        process_table(table)
    except Exception as e:
        logging.error(f"Error processing table {table['table_name']}: {e}")
        continue
    # Uncomment to stop after the first table
    # break
    time.sleep(0.2)
