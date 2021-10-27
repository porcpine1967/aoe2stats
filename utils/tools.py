#!/usr/bin/env python
""" Useful functions. """

from collections import defaultdict
import csv
from datetime import datetime, timedelta, timezone
import json
import requests
import sqlite3

DB = "data/ranked.db"
SEVEN_DAYS_OF_SECONDS = 7 * 24 * 60 * 60

API_TEMPLATE = "https://aoe2.net/api/player/lastmatch?game=aoe2de&profile_id={}"


def user_info(profile_id):
    """ Fetches last match info from aoe2.net"""
    try:
        response = requests.get(API_TEMPLATE.format(profile_id))
        if response.status_code != 200:
            return defaultdict(lambda: "UNKNOWN")
        return json.loads(response.text)
    except:
        return defaultdict(lambda: "UNKNOWN")


def country_map():
    """ Generates a dict for mapping two-letter country codes
to Country names"""
    cmap = {}
    with open("data/countries.csv") as open_file:
        data = csv.reader(open_file)
        for name, code in data:
            cmap[code] = name

    return cmap


def batch(iterable, size=1):
    """ Breaks iterable into chunks. """
    length = len(iterable)
    for ndx in range(0, length, size):
        yield iterable[ndx : min(ndx + size, length)]


def civ_map():
    """ Generates dict for mapping civ id to civ name. """
    cmap = {}
    with open("data/strings.json") as open_file:
        data = json.load(open_file)
    for civ_info in data["civ"]:
        cmap[int(civ_info["id"])] = civ_info["string"]
        cmap[str(civ_info["id"])] = civ_info["string"]

    return cmap


def map_name_lookup():
    """ Returns a dictionary of map_id:map_name pairs. """
    mmap = defaultdict(lambda: "UNKNOWN")
    with open("data/strings.json") as open_file:
        data = json.load(open_file)
    for civ_info in data["map_type"]:
        mmap[str(civ_info["id"])] = civ_info["string"]
        mmap[int(civ_info["id"])] = civ_info["string"]

    return mmap


def map_id_lookup():
    """ Returns a dictionary of map_name:map_id pairs. """
    mmap = {}
    with open("data/strings.json") as open_file:
        data = json.load(open_file)
    for civ_info in data["map_type"]:
        mmap[civ_info["string"]] = civ_info["id"]
        mmap[civ_info["string"].lower()] = civ_info["id"]

    return mmap


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


def execute_transaction(sql):
    """ Wrap sql in commit."""
    conn = sqlite3.connect(DB, timeout=20)
    cur = conn.cursor()
    cur.execute("BEGIN")

    cur.execute(sql)
    cur.execute("COMMIT")


def execute_sql(sql):
    """ Generator for an sql statement and database. """
    conn = sqlite3.connect(DB, timeout=20)
    cur = conn.cursor()
    for row in cur.execute(sql).fetchall():
        yield row
    conn.close()


def all_tuesdays():
    """ All tuesdays in the database."""
    sql = """SELECT DISTINCT date(started, "unixepoch") AS ymd, started FROM matches
    GROUP BY ymd ORDER BY started"""
    tuesdays = set()
    for _, started in execute_sql(sql):
        now = datetime.fromtimestamp(started)
        tuesdays.add(last_time_breakpoint(now))
    return sorted(list(tuesdays))


if __name__ == "__main__":
    for tuesday in all_tuesdays():
        print(tuesday)
