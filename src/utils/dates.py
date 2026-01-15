from datetime import date, datetime, timedelta
from typing import List


def parse_date(value: str) -> date:
    """Convert a date string (YYYY-MM-DD) into date object.
    "2025-01-01" → date(2025, 1, 1)"""
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> List[date]:
    """
    Generate a list of dates from start to end.
    start = 2025-01-01
    end   = 2025-01-03
    → [2025-01-01, 2025-01-02, 2025-01-03]
    """
    if start > end:
        raise ValueError("start_date must be <= end_date")

    days = []
    current = start
    # Loop day by day until we reach the end date
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def get_ingestion_dates(start_date: str, end_date: str) -> List[date]:
    """
    Accepts strings from config.yaml and returns a list of dates.
    -control how many days of data the pipeline will fetch
    """
    start = parse_date(start_date)
    end = parse_date(end_date)
    return date_range(start, end)
