#!/usr/bin/env python
""" Useful functions. """

from datetime import datetime, timedelta, timezone

SEVEN_DAYS_OF_SECONDS = 7 * 24 * 60 * 60


def timeboxes(breakp):
    """ Returns two timebox tuples:
    two weeks before breakpoint to one week before breakpoint
    one week before breakpoint to breakpoint. """
    return (
        (breakp - 2 * SEVEN_DAYS_OF_SECONDS, breakp - SEVEN_DAYS_OF_SECONDS),
        (breakp - SEVEN_DAYS_OF_SECONDS, breakp),
    )


def last_time_breakpoint(now):
    """ Returns datetime of most recent Tuesday at 20:00.\
    If it is Tuesday, it just uses today at 20:00
    n.b.: Monday.weekday() == 0 """
    day_of_week = now.weekday() or 7  # move Monday to 7
    last_tuesday = now - timedelta(days=day_of_week - 1)
    return datetime(
        last_tuesday.year,
        last_tuesday.month,
        last_tuesday.day,
        20,
        tzinfo=timezone.utc,
    )


if __name__ == "__main__":
    n = datetime(2012, 1, 4, tzinfo=timezone.utc)
    print(last_time_breakpoint(n))
