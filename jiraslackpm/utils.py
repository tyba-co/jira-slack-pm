import json
from typing import Union


def print_json(data: Union[dict, list], indent: int = 4) -> None:
    print(json.dumps(data, sort_keys=True, indent=indent, separators=(",", ": ")))