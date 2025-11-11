"""Utility functions for geoacled module."""
import calendar
from datetime import date


def date_range(year: int, month: int) -> tuple[str,str]:
    """Return the first and last day of the given month and year.

    Args:
        year (int): The given month.
        month (int): The given year.

    Returns:
        first, last tuple(str,str): ISO formatted date strings

    """
    first = date(year, month, 1).isoformat()
    last = date(year, month, calendar.monthrange(year, month)[1]).isoformat()
    return first, last
