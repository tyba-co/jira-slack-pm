import jira
import db

def main():

    db.load_new_issues_into_bigquery("k-ren-295903","jira")

    """
    variable = jira.get_all_users()
    print(variable)
    """
    
    
    """
    for user in variable:
        userId = user['accountId']
        issuesUser = jira.get_all_issues_by_user(userId)
        print(issuesUser)
    """
    

if __name__ == "__main__":
    main()