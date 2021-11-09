#!/usr/bin/env python

from datetime import datetime, timedelta, timezone
import pytest

from utils.map_pools import map_type_filter


def test_map_type():
    """ Makes sure grabs the correct category for a week."""
    # 1v1 explicit match
    assert map_type_filter("20210922", 1) == "AND map_type in (9,23,29,71,77,140,167)"
    # 2v2 explicit match
    assert (
        map_type_filter("20210922", 2) == "AND map_type in (9,11,12,29,33,72,74,77,167)"
    )
    # 1v1 offset match
    assert map_type_filter("20210920", 1) == "AND map_type in (9,17,21,29,72,149,161)"
    # 2v2 explicit match
    assert (
        map_type_filter("20210920", 2)
        == "AND map_type in (9,12,29,31,33,77,114,140,166)"
    )
