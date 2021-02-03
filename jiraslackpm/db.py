import sqlite3
import datetime as dt2
from datetime import datetime

import dateutil.parser
import pytz
from google.api_core.exceptions import Conflict, NotFound
import google.cloud.bigquery as bigquery

from jira import get_all_users, get_info_from_issue, get_all_issues_by_user
from utils import get_users_info

class BigQueryDatabase(object):
    """BigQuery database class that holds our data"""

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

    def initialize_tables(
        self, users_table_name: str = "User", issues_table_name: str = "Issue"
    ):
        #ids to check the tables existence
        issues_table_id = "{}.{}".format(self.dataset_id, issues_table_name)
        users_table_id = "{}.{}".format(self.dataset_id, users_table_name)

        #if the user table does exist, it doesn't have to be initialized again.
        try:
            self.client.get_table(users_table_id)
            users = "Users are already uploaded"
        
        #if it doesn't exist, it has to be created with the following schema.
        except NotFound:
            self.delete_table(users_table_name)
            users_schema = [
                bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("account_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("active", "BOOL", mode="REQUIRED"),
                bigquery.SchemaField("display_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("index_date", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            ]
            print("Initializing users table...")
            users = self.create_table(users_table_name, users_schema)
        
        ##if the user table does exist, it doesn't have to be initialized again.
        try:
            self.client.get_table(issues_table_id) 
            issues = "Ready to upload new or updated issues for yesterday"

        #if it doesn't exist, it has to be created with the following schema.
        except NotFound:
            issues_schema = [
                bigquery.SchemaField("story_points", "NUMERIC", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stage", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("priority", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("project_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_summary", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("creator", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("reporter", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("assignee", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("index_date", "TIMESTAMP", mode="REQUIRED")
            ]
            print("Initializing issues table...")
            issues = self.create_table(issues_table_name, issues_schema)
        return users, issues


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

def load_users_into_bigquery(project_id, database_name):
    with BigQueryDatabase(project_id, database_name) as db:
        db.delete_table("User")
        users_table_id = "{}.{}".format(db.dataset_id, "User")
        #if the user table does exist, it doesn't have to be initialized again.
        try:
            db.client.get_table(users_table_id)
            users = "Users are already uploaded"
    
        #if it doesn't exist, it has to be created with the following schema.
        except NotFound:
            users_schema = [
                bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("account_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("active", "BOOL", mode="REQUIRED"),
                bigquery.SchemaField("display_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("index_date", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            ]
            print("Initializing users table...")
            users = db.create_table("User", users_schema)
            users = get_all_users()
            info_users = get_users_info()
            u = datetime.utcnow()
            now = u.replace(tzinfo=pytz.timezone("America/Bogota"))
        
        for user in users:
            if user["accountType"] == "atlassian" and user['active'] == True:
                db.insert_records(
                    "User",
                    [
                        {
                            "account_id": user["accountId"],
                            "account_type": user["accountType"],
                            "active": user["active"],
                            "display_name": user["displayName"],
                            "index_date": str(now),
                            "email": info_users[info_users['id'] == user["accountId"]]['email'].iloc[0],
                        }
                    ],
                    )
                print("Inserted user with ID {} and name {}".format(user["accountId"], user["displayName"]))
                

def load_new_issues_into_bigquery(project_id, database_name):
    with BigQueryDatabase(project_id, database_name) as db:
        users_table, issues_table = db.initialize_tables()
        print(users_table, issues_table)
        users = get_all_users()
        u = datetime.utcnow()
        now = u.replace(tzinfo=pytz.timezone("America/Bogota"))
        for user in users:
            
            if user["accountType"] == "atlassian":

                issues = get_all_issues_by_user(user["accountId"])
                records = []
                for issue in issues:
                    parsed_issue = get_info_from_issue(issue)
                    parsed_issue["assignee"] = user["accountId"]
                    parsed_issue["index_date"] = str(now)
                    created_date = dateutil.parser.parse(parsed_issue["created_at"])
                    updated_date = dateutil.parser.parse(parsed_issue["updated_at"])
                    last_day_date = now - dt2.timedelta(1)
                    if created_date > last_day_date or updated_date > last_day_date:
                        records.append(parsed_issue)
                        
                if records:
                    db.insert_records("Issue", records)
                print(
                    "Inserted {} new issues for user ID: {}".format(
                        len(records), user["accountId"]
                    )
                )
            else:
                print(
                    "User with ID {} is of type {}. Skipping issues fetch..".format(
                        user["accountId"], user["accountType"]
                    )
                )
                
