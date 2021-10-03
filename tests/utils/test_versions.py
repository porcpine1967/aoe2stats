#!/usr/bin/env python

import pytest

from utils.versions import version_for_date, version_for_timestamp


def test_version_for_date():
    """ Tests version_for_date """
    assert version_for_date("20201125") == "43210"
    assert version_for_date("20221125") == "53347"
    assert version_for_date("19990101") is None


def test_version_for_timestamp():
    """ Tests version_for_timestamp """
    assert version_for_timestamp(1606329267) == "43210"
    assert version_for_timestamp(1633286072) == "53347"
    assert version_for_timestamp(943554867) is None
