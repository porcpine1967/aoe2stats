#!/usr/bin/env python
""" Useful functions. """

from collections import defaultdict
import csv
from datetime import datetime, timedelta, timezone
import json

import psycopg2
import requests

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

def tournament_timeboxes(now):
    breakpoint = last_time_breakpoint(now).date()
    return (
        (breakpoint - timedelta(days=7), breakpoint - timedelta(days=1),),
        (breakpoint, breakpoint + timedelta(days=6),),
        )

def timeboxes(breakp):
    """ Returns two timebox tuples:
    two weeks before breakpoint to one week before breakpoint
    one week before breakpoint to breakpoint. """
    return (
        (breakp - 2 * SEVEN_DAYS_OF_SECONDS, breakp - SEVEN_DAYS_OF_SECONDS),
        (breakp - SEVEN_DAYS_OF_SECONDS, breakp),
    )

def last_time_breakpoint(now):
    """ Returns datetime of most recent Wednesday at 1:00.\
    n.b.: Monday.weekday() == 0 """
    day_of_week = now.weekday()
    if day_of_week < 2:
        day_of_week += 7
    last_wednesday = now - timedelta(days=day_of_week - 2)
    return datetime(
        last_wednesday.year,
        last_wednesday.month,
        last_wednesday.day,
        1,
        tzinfo=timezone.utc,
    )

def weekend(now):
    """ Returns timebox of weekend (FRI-MON) before "now" """
    now_midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_of_week = now_midnight.weekday()

    friday = now_midnight - timedelta(days=day_of_week + 3)
    friday_ts = friday.timestamp()
    monday_ts = (friday + timedelta(days=3, hours=7)).timestamp()
    return (friday_ts, monday_ts)

def execute_transaction(sql, db_path=DB):
    """ Wrap sql in commit."""
    conn = psycopg2.connect(database="aoe2stats")
    cur = conn.cursor()
    cur.execute("BEGIN")

    cur.execute(sql)
    cur.execute("COMMIT")

def execute_sql(sql, db_path=DB):
    """ Generator for an sql statement and database. """
    conn = psycopg2.connect(database="aoe2stats")
    cur = conn.cursor()
    cur.execute(sql)
    for row in cur.fetchall():
        yield row
    cur.close()
    conn.close()


def all_wednesdays():
    """ All wednesdays in the database."""
    sql = """SELECT DISTINCT to_timestamp(started)::date AS ymd, max(started) FROM matches
where started > 1635603919
    GROUP BY ymd ORDER BY ymd"""
    wednesdays = set()
    for _, started in execute_sql(sql):
        now = datetime.utcfromtimestamp(started)
        wednesdays.add(last_time_breakpoint(now))
    return sorted(list(wednesdays))


if __name__ == "__main__":
    for wednesday in all_wednesdays():
        print(wednesday)
