#!/usr/bin/env python

""" Downloads data from aoe2.net and adds to local db. """

from argparse import ArgumentParser
from collections import Counter
from datetime import datetime, timedelta
import json
import sqlite3
import sys
import time

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from utils.match_collation import MatchCollator
from utils.update import CREATE_MATCH_TABLE, save_matches
from utils.tools import execute_sql, execute_transaction
from utils.tools import SEVEN_DAYS_OF_SECONDS

UNRANKED_DB = "data/unranked_4.db"
CREATE_PLAYER_TABLE = """CREATE TABLE IF NOT EXISTS players (
id integer PRIMARY KEY,
player_id integer,
leaderboard_id integer,
games integer,
UNIQUE(player_id, leaderboard_id))"""


LEADERBOARD = 17
RH_API = "https://aoeiv.net/api/player/ratinghistory?game=aoe4&leaderboard_id={}&profile_id={}&count={}"
RH_MAX = 1000
MATCHES_API = "https://aoeiv.net/api/player/matches?game=aoe4&profile_id={}&count={}"
MATCHES_MAX = 10000
LB_API = (
    "https://aoeiv.net/api/leaderboard?game=aoe4&leaderboard_id={}&start={}&count={}"
)
LB_MAX = 10000
LEADERBOARD_SQL = """SELECT player_id FROM players
WHERE player_id = {}
AND leaderboard_id = {}
AND games = {}"""
UPDATE_LEADERBOARD_SQL = """INSERT OR REPLACE INTO players
(player_id, leaderboard_id, games)
VALUES ({pid}, {lid}, {games})"""


def prepare_database():
    """ Creates the db file if not exists. Creates table if not exists. """
    for database in (UNRANKED_DB,):
        conn = sqlite3.connect(database)
        cur = conn.cursor()
        cur.execute(CREATE_MATCH_TABLE)
        cur.execute(CREATE_PLAYER_TABLE)
        conn.close()


def download(url):
    """ Fetches data from a url."""
    retry_strategy = Retry(backoff_factor=10, total=6)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    response = http.get(url)
    if response.status_code != 200:
        print(response.text)
        sys.exit(1)
    return response


def player_changed(blob):
    """Checks if player information has changed since last run."""
    sql = LEADERBOARD_SQL.format(blob["profile_id"], LEADERBOARD, blob["games"])
    for _ in execute_sql(sql, UNRANKED_DB):
        return False
    return True


def fetch_users(page=1):
    """Fetches a leaderboard worth of users.
Filters out users already up to date."""
    url = LB_API.format(LEADERBOARD, 1 + LB_MAX * (page - 1), LB_MAX)
    response = download(url)
    data = json.loads(response.text)
    users = list()
    for player in data["leaderboard"]:
        if player_changed(player):
            users.append(player)
    return data["total"], users


def fetch_matches(profile_id):
    """ Gets latest match info for user."""
    url = MATCHES_API.format(profile_id, MATCHES_MAX)
    response = download(url)
    return json.loads(response.text)


def fetch_ratings_history(profile_id):
    """ Gets ratings history for user."""
    url = RH_API.format(LEADERBOARD, profile_id, RH_MAX)
    response = download(url)
    return json.loads(response.text)


def fetch_and_save():
    """ Fetches up to one week of data from start. """
    page_count = 2
    page = 1
    ctr = 3519
    while page < page_count:
        total, users = fetch_users(page)
        for user in users:
            ctr += 1
            print("{:10}".format(ctr), end="\r")
            profile_id = user["profile_id"]
            matches = fetch_matches(profile_id)
            ratings = fetch_ratings_history(profile_id)
            collator = MatchCollator(profile_id)
            collator.collate(matches, ratings)
            save_matches(collator.match_rows, UNRANKED_DB)
            sql = UPDATE_LEADERBOARD_SQL.format(
                lid=LEADERBOARD, pid=profile_id, games=user["games"]
            )
            execute_transaction(sql, UNRANKED_DB)
        page_count = 1 + total / LB_MAX
        page += 1


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

    parser.add_argument(
        "--end", help="End time in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-ts", type=int, help="End time as UNIX timestamp",
    )

    parser.add_argument("--lw", action="store_true", help="Reload the last week")

    args = parser.parse_args()
    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        start_timestamp = int(start_date.timestamp())
    elif args.start_ts:
        start_timestamp = args.start_ts

    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
        end_timestamp = int(end_date.timestamp())
    elif args.end_ts:
        end_timestamp = args.end_ts
    else:
        end_timestamp = None

    fetch_and_save()


if __name__ == "__main__":
    run()
