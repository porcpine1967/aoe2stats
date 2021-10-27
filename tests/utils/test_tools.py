#!/usr/bin/env python

from datetime import datetime, timedelta, timezone
import pytest

import utils.tools


def test_last_time_breakpoint():
    """ Tests last_time_breakpoint."""
    expected = datetime(2012, 1, 4, 1, tzinfo=timezone.utc)
    for i in range(4, 11):
        now = datetime(2012, 1, i)
        assert utils.tools.last_time_breakpoint(now) == expected

    now = datetime(2012, 1, 2)
    expected = datetime(2011, 12, 28, 1, tzinfo=timezone.utc)
    assert utils.tools.last_time_breakpoint(now) == expected


def test_timeboxes():
    """ Tests timeboxes method. """
    breakp = datetime(2012, 1, 3, 20, tzinfo=timezone.utc)
    midpoint = int(datetime.timestamp(breakp - timedelta(days=7)))
    start = int(datetime.timestamp(breakp - timedelta(days=14)))
    breakp_ts = int(datetime.timestamp(breakp))
    expected = (
        (start, midpoint,),
        (midpoint, breakp_ts,),
    )
    assert utils.tools.timeboxes(breakp_ts) == expected
