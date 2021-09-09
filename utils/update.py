#!/usr/bin/env python

""" Downloads data from aoe2.net and adds to local db. """

from argparse import ArgumentParser
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import sys
import time

import requests

RANKED_DB = "data/ranked.db"
UNRANKED_DB = "data/unranked.db"
CREATE_MATCH_TABLE = """CREATE TABLE IF NOT EXISTS matches (
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
UNIQUE(match_id, player_id))"""

MAX_DOWNLOAD = 1000

API_TEMPLATE = "https://aoe2.net/api/matches?game=aoe2de&count={count}&since={start}"

BATCH_SIZE = 300


def latest_version():
    """ Returns latest version available in db. """
    conn = sqlite3.connect(RANKED_DB)
    cur = conn.cursor()
    version = 0
    for (current_version,) in cur.execute(
        "SELECT DISTINCT version FROM matches WHERE version IS NOT NULL"
    ).fetchall():
        if int(current_version) > version:
            version = int(current_version)
    conn.close()
    return version


DEFAULT_VERSION = latest_version()

GAME_TYPE_TO_RATING_TYPE = {
    "1v1": {0: 2, 2: 1, 12: 9, 13: 13},
    "team": {0: 4, 2: 3, 12: 9, 13: 14},
}


def one_week_ago():
    """ Returns unix timestamp of one week ago. """
    week = timedelta(days=7)
    return int(datetime.timestamp((datetime.now() - week)))


def prepare_database():
    """ Creates the db file if not exists. Creates table if not exists. """
    for db in (
        RANKED_DB,
        UNRANKED_DB,
    ):
        conn = sqlite3.connect(db)
        cur = conn.cursor()
        cur.execute(CREATE_MATCH_TABLE)
        conn.close()


def last_match_time():
    """ Returns the time of the last match retrieved or one week ago. """
    conn = sqlite3.connect(RANKED_DB)
    cur = conn.cursor()
    query = cur.execute("SELECT MAX(started) FROM matches")
    result = query.fetchone()[0]
    conn.close()
    cutoff = one_week_ago()
    # results trickle in, so start 90 minutes before end of last run
    return result and result > cutoff and result - 5400 or cutoff


def batch(iterable, size=1):
    """ Breaks iterable into chunks. """
    length = len(iterable)
    for ndx in range(0, length, size):
        yield iterable[ndx : min(ndx + size, length)]


def save_matches(matches, database):
    """ Inserts each match value into the database. """
    sql = """ INSERT OR IGNORE INTO matches
(match_id, map_type, rating_type, version, started,
player_id, civ_id, rating, won, mirror)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    for match_batch in batch(matches, BATCH_SIZE):
        cur.execute("BEGIN")

        for match in match_batch:
            cur.execute(sql, match)
        cur.execute("COMMIT")
    conn.close()


def deduce_rating_type(match):
    """ Deduces rating type based on match data"""
    # Only deduce if unranked, which has rating_type 0
    if match["rating_type"]:
        return match["rating_type"]
    try:
        if match["num_players"] > 2:
            return GAME_TYPE_TO_RATING_TYPE["team"][match["game_type"]]
        return GAME_TYPE_TO_RATING_TYPE["1v1"][match["game_type"]]
    except KeyError:
        return None


def fetch_matches(start):
    """ Fetches match data via one api call starting at start_time.
        Returns number of matches, latest start time, and
        array of values ready for sql insert. """

    start_time = datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M")
    print("FETCHING from {} ({})".format(start_time, start))

    ranked_match_data = []
    unranked_match_data = []

    url = API_TEMPLATE.format(start=start, count=MAX_DOWNLOAD)
    response = requests.get(url)
    if response.status_code != 200:
        print(response.text)
        sys.exit(1)
    next_start = start
    data = json.loads(response.text)
    match_ids = set()
    for match in data:
        rating_type = deduce_rating_type(match)
        # ignore if no map_type, no rating_type
        if not match["map_type"] or not rating_type:
            continue
        try:
            validate_player_info(match)
        except PlayerInfoException:
            continue

        unranked = match["rating_type"] == 0
        match_rows = []

        row = [
            match["match_id"],
            match["map_type"],
            rating_type,
            match["version"] or DEFAULT_VERSION,
            match["started"],
        ]
        if match["started"] > next_start:
            next_start = match["started"]
        civs = set()
        for player in match["players"]:
            player_row = [
                player["profile_id"],
                player["civ"],
                player["rating"],
                player["won"],
            ]
            match_rows.append(row + player_row)
            civs.add(player["civ"])

        # Determine if it is a mirror match (all have same civs)
        for row in match_rows:
            row.append(len(civs) == 1)

        if unranked:
            unranked_match_data += match_rows
        else:
            ranked_match_data += match_rows
        match_ids.add(match["match_id"])
    print("Match count:", len(match_ids))
    return len(data), next_start, ranked_match_data, unranked_match_data


class PlayerInfoException(Exception):
    """ Exception thrown when match player info invalid."""


def validate_player_info(match):
    """ Raises PlayerInfoException if player info makes match invalid for analysis."""
    if match["num_players"] != len(match["players"]):
        raise PlayerInfoException("num_players not match number of players")
    if match["num_players"] % 2:
        raise PlayerInfoException("odd number of players")
    teams = Counter()
    wins = Counter()
    for player in match["players"]:
        if "civ" not in player:
            raise PlayerInfoException("Player missing civ")
        if "profile_id" not in player:
            raise PlayerInfoException("Player missing profile_id")
        teams[player["team"]] += 1
        wins[player["won"]] += 1
    if len(teams) != 2:
        raise PlayerInfoException("Unusual number of teams: {}".format(len(teams)))
    if len(set(teams.values())) != 1:
        raise PlayerInfoException("Teams not evenly divided")
    if len(wins) != 2:
        raise PlayerInfoException(
            "Unreasonable number of win conditions: {}".format(len(wins))
        )
    if len(set(wins.values())) != 1:
        raise PlayerInfoException("Wins not evenly divided")


def time_left(script_start, pct):
    """ Returns string version of hours:minutes:seconds probably left. """
    now = datetime.now()
    now_ts = now.timestamp()
    seconds_to_run = (now_ts - script_start) / pct
    estimated_end_ts = script_start + seconds_to_run
    seconds_left = int(estimated_end_ts - now_ts)
    time_remaining = timedelta(seconds=seconds_left)
    estimated_end = now + time_remaining
    return "Time Remaining: {}\nEstimated end: {}".format(
        str(time_remaining), estimated_end.strftime("%H:%M")
    )


def fetch_and_save(start):
    """ Fetches up to one week of data from start. """
    script_start = datetime.now().timestamp()
    print("Starting at {}".format(int(script_start)))
    hold_start = 0
    fetch_start = start
    data_length = MAX_DOWNLOAD
    week = 604800
    week_ago = start + week
    expected_end = script_start if script_start < week_ago else week_ago
    while fetch_start > hold_start and fetch_start - start < week:
        hold_start = fetch_start
        data_length, fetch_start, ranked, unranked = fetch_matches(fetch_start)
        save_matches(ranked, RANKED_DB)
        save_matches(unranked, UNRANKED_DB)
        if data_length < MAX_DOWNLOAD:
            break
        print("sleeping...")
        time.sleep(10)
        pct = float(fetch_start - start) / (expected_end - start)
        print(time_left(script_start, pct))
    print("Ending at {}".format(datetime.now().strftime("%H:%M")))


def run():
    """ Parses arguments and runs the appropriate functions. """
    prepare_database()
    parser = ArgumentParser()
    parser.add_argument(
        "--start", help="Start time in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--start-ts", type=int, help="Start time as UNIX timestamp",
    )

    args = parser.parse_args()
    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        start_timestamp = int(start_date.timestamp())
    elif args.start_ts:
        start_timestamp = args.start_ts
    else:
        start_timestamp = last_match_time()
    fetch_and_save(start_timestamp)


if __name__ == "__main__":
    run()
