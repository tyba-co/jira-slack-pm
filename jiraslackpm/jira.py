import os
from typing import Optional

import dateutil.parser
import requests
from pydash import get as s_get
from requests.auth import HTTPBasicAuth

from utils import print_json

api_token = os.environ["JIRA_API_TOKEN"]
AUTH = HTTPBasicAuth(os.environ["JIRA_API_EMAIL"], api_token)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
PARAMS = {}
BASE_URL = "https://starkmvp.atlassian.net/rest/api/3/"


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
        if issues.get("issues"):
            response += issues.get("issues")
            offset += 50
        else:
            break
    return response


def get_issues_in_current_week_by_user(account_id: str, pprint=False) -> list:
    uri = BASE_URL + "search"
    response = []
    offset = 0
    while True:
        query = {
            "jql": "assignee = {} and created >= startOfWeek()".format(account_id),
            "startAt": offset,
        }
        issues = call_api(uri, params=query)
        if pprint:
            print_json(issues)
        if issues.get("issues"):
            response += issues.get("issues")
            offset += 50
        else:
            break
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
        "stage": s_get(issue, "fields.status.name"),
        "priority": s_get(issue, "fields.priority.name"),
        "issue_id": issue.get("id"),
        "issue_name": issue.get("key"),
        "project_name": s_get(issue, "fields.project.name"),
        "issue_summary": s_get(issue, "fields.summary"),
        "creator": s_get(issue, "fields.creator.accountId"),
        "reporter": s_get(issue, "fields.reporter.accountId"),
        "created_at": str(dateutil.parser.parse(s_get(issue, "fields.created"))),
        "updated_at": str(dateutil.parser.parse(s_get(issue, "fields.updated"))),
        "issue_type": s_get(issue, "fields.issuetype.name"),
    }
