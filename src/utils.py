"""
This file contains some functions that are shared
across cleaning modules
"""
import re


def _shorten_str(x):
    """Finds bracketed number in strings and deletes brackets and number."""
    if type(x) == str:
        return re.sub(r"\[-?\d*\] ", "", x)
    else:
        return x


def remove_brackets_in_categorical_values(data):
    """Applies _shorten_str to a dataframe."""
    for c in data:
        data[c] = data[c].apply(lambda x: _shorten_str(x))
    return data
