"""
Sale period detection for smart cache invalidation.
During major sales, prices change rapidly so we skip caching.
"""

from datetime import datetime, date
from typing import Tuple, Optional


# Define sale periods as (start_month, start_day, end_month, end_day, name)
# These are approximate windows when deals change frequently
SALE_PERIODS = [
    # Black Friday week (Thanksgiving is 4th Thursday of November, so we cover the whole week)
    (11, 20, 11, 30, "Black Friday Week"),

    # Cyber Monday (extends a few days after)
    (12, 1, 12, 3, "Cyber Monday"),

    # Prime Day (typically mid-July, 2 days)
    (7, 10, 7, 17, "Prime Day"),

    # Holiday shopping surge
    (12, 15, 12, 26, "Holiday Season"),

    # Post-holiday clearance
    (12, 26, 12, 31, "Post-Holiday Clearance"),
    (1, 1, 1, 5, "New Year Clearance"),

    # Presidents Day weekend sales
    (2, 14, 2, 21, "Presidents Day"),

    # Memorial Day weekend
    (5, 24, 5, 31, "Memorial Day"),

    # Labor Day weekend
    (9, 1, 9, 7, "Labor Day"),
]


def is_sale_period(check_date: Optional[date] = None) -> Tuple[bool, Optional[str]]:
    """
    Check if the given date falls within a major sale period.

    Args:
        check_date: Date to check (defaults to today)

    Returns:
        Tuple of (is_sale_period, sale_name)
    """
    if check_date is None:
        check_date = date.today()

    month = check_date.month
    day = check_date.day

    for start_month, start_day, end_month, end_day, name in SALE_PERIODS:
        # Handle periods that cross year boundary (like Dec 26 - Jan 5)
        if start_month > end_month:
            # Period crosses year boundary
            if (month == start_month and day >= start_day) or \
               (month == end_month and day <= end_day) or \
               (month > start_month) or (month < end_month):
                return True, name
        else:
            # Normal period within same year
            if (month == start_month and day >= start_day and
                (month < end_month or (month == end_month and day <= end_day))):
                return True, name
            elif (month > start_month and month < end_month):
                return True, name
            elif (month == end_month and day <= end_day and month > start_month):
                return True, name
            # Simpler check for same-month periods
            elif start_month == end_month and month == start_month:
                if start_day <= day <= end_day:
                    return True, name

    return False, None


def get_cache_ttl_hours() -> int:
    """
    Get the appropriate cache TTL based on current date.

    Returns:
        Cache TTL in hours (0 means no caching / always fresh)
    """
    is_sale, sale_name = is_sale_period()

    if is_sale:
        # During sales, always do fresh searches
        return 0

    # Normal period: cache for 48 hours
    return 48


def get_cache_status() -> dict:
    """
    Get current cache status for debugging/display.

    Returns:
        Dict with cache configuration info
    """
    is_sale, sale_name = is_sale_period()
    ttl = get_cache_ttl_hours()

    return {
        "is_sale_period": is_sale,
        "sale_name": sale_name,
        "cache_ttl_hours": ttl,
        "cache_enabled": ttl > 0,
        "checked_at": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    # Test the module
    status = get_cache_status()
    print(f"Current cache status:")
    for key, value in status.items():
        print(f"  {key}: {value}")
