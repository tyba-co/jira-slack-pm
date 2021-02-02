import json
import pandas as pd
from typing import Union


def print_json(data: Union[dict, list], indent: int = 4) -> None:
    print(json.dumps(data, sort_keys=True, indent=indent, separators=(",", ": ")))

def get_users_info() -> pd.DataFrame:
    data = pd.read_csv("./data/export-users.csv")
    groupby_cols = ['id', 'name', 'email','active']
    users_data = data.groupby(groupby_cols).count()
    users_df = pd.DataFrame.from_records(users_data.index, columns = groupby_cols)
    active_users = users_df[users_df['active'] == 'Yes']
    
    return active_users

