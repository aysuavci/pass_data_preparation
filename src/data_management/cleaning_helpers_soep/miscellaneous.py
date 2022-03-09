"""
This file contains some functions that are helpful for cleaning files
and don't fit in any of the other categories
"""
import sys
from traceback import format_exception


def get_traceback():
    tb = format_exception(*sys.exc_info())
    if isinstance(tb, list):
        tb = "".join(tb)
    return tb
