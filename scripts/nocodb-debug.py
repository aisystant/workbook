#!/usr/bin/env python

import os
import requests
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

API_URL = os.getenv("NOCO_API_URL")
API_TOKEN = os.getenv("NOCO_API_TOKEN")
HEADERS = {"xc-token": API_TOKEN}


def get_tables(base_id):
    r = requests.get(f"{API_URL}/api/v2/meta/bases/{base_id}/tables", headers=HEADERS)
    r.raise_for_status()
    return r.json().get("list", [])


def get_records(table_id, limit=1000):
    r = requests.get(f"{API_URL}/api/v2/tables/{table_id}/records?limit={limit}", headers=HEADERS)
    r.raise_for_status()
    return r.json().get("list", [])


def main():
    print("== NocoDB Workspaces, Bases, Tables, Records ==")
    workspaces = get_workspaces()
    for ws in workspaces:
        ws_id = ws["id"]
        ws_title = ws.get("title", "Unnamed")
        print(f"\n🗂️ Workspace: {ws_title} ({ws_id})")

        bases = get_bases(ws_id)
        for base in bases:
            base_id = base["id"]
            base_title = base.get("title", "Untitled Base")
            print(f"  📁 Base: {base_title} ({base_id})")

            tables = get_tables(base_id)
            for table in tables:
                table_id = table["id"]
                table_title = table.get("title", "Untitled Table")
                table_name = table.get("table_name")
                print(f"    📄 Table: {table_title} | ID: {table_id} | Name: {table_name}")

                records = get_records(table_id)
                for idx, record in enumerate(records, 1):
                    print(f"      🔹 Record {idx}: {record}")


if __name__ == "__main__":
    main()