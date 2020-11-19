import sqlite3
from datetime import datetime
from typing import Optional, Union

import pytz
import requests
from google.api_core.exceptions import Conflict, NotFound
from requests.auth import HTTPBasicAuth
import json
from pydash import get as s_get
from google.cloud import bigquery
import os


api_token = os.environ["JIRA_API_TOKEN"]
AUTH = HTTPBasicAuth(os.environ["JIRA_API_EMAIL"], api_token)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
PARAMS = {}
BASE_URL = "https://starkmvp.atlassian.net/rest/api/3/"


class SQLiteDatabase(object):
    """sqlite3 database class that holds our data"""

    def __init__(self, db_name):
        """Initialize db class variables"""
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

    def __del__(self):
        self.connection.close()

    def close(self):
        self.connection.close()

    def execute(self, sql):
        self.cursor.execute(sql)

    def drop_table(self, table_name):
        self.cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))

    def commit(self):
        """commit changes to database"""
        self.connection.commit()


class BigQueryDatabase(object):
    """sqlite3 database class that holds our data"""

    def __init__(self, project_id, db_name):
        """Initialize db class variables"""
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = "{}.{}".format(self.client.project, db_name)
        try:
            dataset = bigquery.Dataset(self.dataset_id)
            dataset = self.client.create_dataset(
                dataset, timeout=30
            )  # Make an API request.
            self.dataset = dataset
        except Conflict:
            self.dataset = self.client.get_dataset(self.dataset_id)

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.client = None
        self.dataset = None

    def __del__(self):
        self.client = None
        self.dataset = None

    def close(self):
        self.client = None
        self.dataset = None

    def create_table(self, table_name: str, schema: list):
        table_id = "{}.{}".format(self.dataset_id, table_name)
        try:
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
        except Conflict:
            table = self.client.get_table(table_id)
        return table

    def delete_table(self, table_name: str):
        table_id = "{}.{}".format(self.dataset_id, table_name)
        try:
            self.client.delete_table(table_id)
        except NotFound:
            print("Table already deleted...")

    def insert_records(self, table_name, records: list):
        table_id = "{}.{}".format(self.dataset_id, table_name)
        errors = self.client.insert_rows_json(table_id, records)
        if not errors:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def initialize_tales(
        self, users_table_name: str = "User", issues_table_name: str = "Issue"
    ):
        self.delete_table(users_table_name)
        users_schema = [
            bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("account_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("active", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("display_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        ]
        print("Initializing users table...")
        users = self.create_table(users_table_name, users_schema)
        self.delete_table(issues_table_name)
        issues_schema = [
            bigquery.SchemaField("story_points", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("priority", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("issue_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("issue_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("project_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("issue_summary", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("creator", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reporter", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("issue_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        print("Initializing issues table...")
        issues = self.create_table(issues_table_name, issues_schema)
        return users, issues

    def execute(self, sql):
        self.cursor.execute(sql)

    def drop_table(self, table_name):
        self.cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))

    def commit(self):
        """commit changes to database"""
        self.connection.commit()


def print_json(data: Union[dict, list], indent: int = 4) -> None:
    print(json.dumps(data, sort_keys=True, indent=indent, separators=(",", ": ")))


def call_api(uri: str, method="GET", headers=None, auth=AUTH, params=None) -> dict:
    if params is None:
        params = PARAMS
    if headers is None:
        headers = HEADERS
    response = requests.request(method, uri, headers=headers, params=params, auth=auth)
    return response.json()


def get_all_users(pprint: bool = False) -> list:
    response = []
    uri = BASE_URL + "users/search"
    offset = 0
    condition = True
    while condition:
        params = {"startAt": offset}
        users = call_api(uri, params=params)
        if pprint:
            print_json(users)
        if users:
            response += users
            offset += 50
        else:
            condition = False
    return response


def get_all_issues_by_user(account_id: str, pprint=False) -> list:
    uri = BASE_URL + "search"
    response = []
    offset = 0
    while True:
        query = {"jql": "assignee = {}".format(account_id), "startAt": offset}
        issues = call_api(uri, params=query)
        if pprint:
            print_json(issues)
        if issues.get('issues'):
            response += issues.get('issues')
            offset += 50
        else:
            break
    return response


def get_issues_in_current_week_by_user(account_id: str, pprint=False) -> list:
    uri = BASE_URL + "search"
    response = []
    offset = 0
    condition = True
    while condition:
        query = {
            "jql": "assignee = {} and created >= startOfWeek()".format(account_id),
            "startAt": offset,
        }
        issues = call_api(uri, params=query)
        if pprint:
            print_json(issues)
        if issues:
            response += issues
            offset += 50
        else:
            condition = False
    return response


def get_sp_brute_force(fields: dict, is_custom_field=False) -> Optional[int]:
    for k, v in fields.items():
        if isinstance(v, float) or isinstance(v, int):
            if is_custom_field:
                if "customfield" in k:
                    return v
            else:
                return v
    return


def get_info_from_issue(issue: dict) -> dict:
    return {
        "story_points": get_sp_brute_force(
            issue.get("fields", {}), is_custom_field=True
        ),
        "status": s_get(issue, "fields.status.statusCategory.name"),
        "priority": s_get(issue, "fields.priority.name"),
        "issue_id": issue.get("id"),
        "issue_name": issue.get("key"),
        "project_name": s_get(issue, "fields.project.name"),
        "issue_summary": s_get(issue, "fields.summary"),
        "creator": s_get(issue, "fields.creator.accountId"),
        "reporter": s_get(issue, "fields.reporter.accountId"),
        "created_at": s_get(issue, "fields.created"),
        "updated_at": s_get(issue, "fields.updated"),
        "issue_type": s_get(issue, "fields.issuetype.name"),
    }


def load_into_bigquery(project_id, database_name):
    with BigQueryDatabase(project_id, database_name) as db:
        users_table, issues_table = db.initialize_tales()
        print(users_table, issues_table)
        users = get_all_users()[:10]
        u = datetime.utcnow()
        now = u.replace(tzinfo=pytz.timezone("America/Bogota"))
        for user in users:
            db.insert_records(
                "User",
                [
                    {
                        "account_id": user["accountId"],
                        "account_type": user["accountType"],
                        "active": user["active"],
                        "display_name": user["displayName"],
                        "updated_at": str(now),
                        "email": None,
                    }
                ],
            )
            print("Inserted user with ID {} and name {}".format(user["accountId"], user["displayName"]))
            if user["accountType"] == "atlassian":
                issues = get_all_issues_by_user(user["accountId"])
                records = []
                for issue in issues:
                    records.append(get_info_from_issue(issue))
                if records:
                    db.insert_records("Issue", records)
                print(
                    "Inserted {} issues for user ID: {}".format(
                        len(records), user["accountId"]
                    )
                )
            else:
                print(
                    "User with ID {} is of type {}. Skipping issues fetch..".format(
                        user["accountId"], user["accountType"]
                    )
                )


load_into_bigquery("k-ren-295903", "jira")
