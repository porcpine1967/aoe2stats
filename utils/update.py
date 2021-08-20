#!/usr/bin/env python

""" Downloads data from aoe2.net and adds to local db. """

from argparse import ArgumentParser
from datetime import datetime, timedelta
import json
import sqlite3
import sys
import time

import requests

DB = "data/aoe2.net.db"

MATCH_TABLE = "matches"

CREATE_MATCH_TABLE = """CREATE TABLE IF NOT EXISTS {} (
id integer PRIMARY KEY,
match_id text,
player_id integer,
rating integer,
civ_id integer,
map_type integer,
rating_type integer,
started integer,
version text,
won integer,
mirror integer,
UNIQUE(match_id, player_id))""".format(
    MATCH_TABLE
)

MAX_DOWNLOAD = 1000

API_TEMPLATE = "https://aoe2.net/api/matches?game=aoe2de&count={count}&since={start}"

BATCH_SIZE = 300


def one_week_ago():
    """ Returns unix timestamp of one week ago. """
    week = timedelta(days=7)
    return int(datetime.timestamp((datetime.now() - week)))


def prepare_database():
    """ Creates the db file if not exists. Creates table if not exists. """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(CREATE_MATCH_TABLE)
    conn.close()


def last_match_time():
    """ Returns the time of the last match retrieved or one week ago. """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    query = cur.execute("SELECT MAX(started) FROM {}".format(MATCH_TABLE))
    result = query.fetchone()[0]
    conn.close()
    cutoff = one_week_ago()
    # results trickle in, so start 1 hour before end of last run
    return result and result > cutoff and result - 3600 or cutoff


def batch(iterable, size=1):
    """ Breaks iterable into chunks. """
    length = len(iterable)
    for ndx in range(0, length, size):
        yield iterable[ndx : min(ndx + size, length)]


def save_matches(matches):
    """ Inserts each match value into the database. """
    sql = """ INSERT OR IGNORE INTO {}
(match_id, map_type, rating_type, version, started,
player_id, civ_id, rating, won, mirror)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".format(
        MATCH_TABLE
    )
    print(len(matches))
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    for match_batch in batch(matches, BATCH_SIZE):
        print("SAVING BATCH")
        cur.execute("BEGIN")

        for match in match_batch:
            cur.execute(sql, match)
        cur.execute("COMMIT")
    conn.close()


def fetch_matches(start):
    """ Fetches match data via one api call starting at start_time.
        Returns number of matches, latest start time, and
        array of values ready for sql insert. """
    print(
        "FETCHING from {} ({})".format(
            datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M"), start
        )
    )
    match_data = []
    response = requests.get(API_TEMPLATE.format(start=start, count=MAX_DOWNLOAD))
    if response.status_code != 200:
        print(response.text)
        sys.exit(1)
    next_start = start
    data = json.loads(response.text)
    for match in data:
        # ignore unranked
        if match["rating_type"] == 0:
            continue
        # ignore if not version
        if not match["version"]:
            continue
        # ignore if not map_type
        if not match["map_type"]:
            continue
        match_rows = []
        have_winner = False
        row = [
            match["match_id"],
            match["map_type"],
            match["rating_type"],
            match["version"],
            match["started"],
        ]
        if match["started"] > next_start:
            next_start = match["started"]
        civs = set()
        for player in match["players"]:
            if not player["profile_id"]:
                continue
            if not player["civ"]:
                continue
            if player["won"]:
                have_winner = True
            player_row = [
                player["profile_id"],
                player["civ"],
                player["rating"],
                player["won"],
            ]
            match_rows.append(row + player_row)
            civs.add(player["civ"])
        # Make sure have even number of players and someone won
        if len(match_rows) % 2 or not have_winner:
            continue
        # Determine if it is a mirror match (all have same civs)
        if len(civs) > 1:
            for row in match_rows:
                row.append(False)
        else:
            for row in match_rows:
                row.append(True)
        match_data += match_rows

    return len(data), next_start, match_data


def time_left(script_start, pct):
    """ Returns string version of hours:minutes:seconds probably left. """
    now = datetime.now().timestamp()
    seconds_to_run = (now - script_start) / pct
    estimated_end = script_start + seconds_to_run
    seconds_left = int(estimated_end - now)
    return "Time Remaining: {}".format(str(timedelta(seconds=seconds_left)))


def fetch_and_save(start):
    """ Fetches up to one week of data from start. """
    script_start = datetime.now().timestamp()
    print("Starting at {}".format(int(script_start)))
    hold_start = 0
    fetch_start = start
    data_length = MAX_DOWNLOAD
    week = 604800
    expected_end = script_start if start + week > script_start else start + week
    while fetch_start > hold_start and fetch_start - start < week:
        hold_start = fetch_start
        data_length, fetch_start, match_data = fetch_matches(fetch_start)
        save_matches(match_data)
        if data_length < MAX_DOWNLOAD:
            break
        time.sleep(10)
        print(
            time_left(script_start, float(fetch_start - start) / (expected_end - start))
        )
    print("Ending at {}".format(int(datetime.now().timestamp())))


def run(start):
    """ Parses arguments and runs the appropriate functions. """
    prepare_database()
    fetch_and_save(start)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--start", help="Start time in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--start-ts", type=int, help="Start time as UNIX timestamp",
    )

    args = parser.parse_args()
    if args.start:
        start_timestamp = int(datetime.strptime(args.start, "%Y-%m-%d").timestamp())
    elif args.start_ts:
        start_timestamp = args.start_ts
    else:
        start_timestamp = last_match_time()
    run(start_timestamp)
