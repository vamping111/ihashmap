from types import FunctionType
from typing import Any, List, Mapping


def match_query(
    value: Mapping[str, Any],
    query: Mapping[str, Any],
) -> List[Mapping[str, Any]]:
    """Matches query to mapping values.

    :param value: mapping value.
    :param query: pattern to match against value
    """

    matched = []
    match = {key: False for key in query}

    for search_key, search_value in query.items():
        if isinstance(search_value, FunctionType):
            if search_value(value.get(search_key)):
                match[search_key] = True
        else:
            if value.get(search_key) == search_value:
                match[search_key] = True

    if all(match.values()):
        matched.append(value)

    return matched
