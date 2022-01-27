#!/usr/bin/env python

""" Downloads data from aoe2.net and adds to local db. """

from argparse import ArgumentParser
from collections import Counter
from datetime import datetime, timedelta
import json
import psycopg2
import psycopg2.extras
import sys
import time

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from utils.results_cacher import generate_results
from utils.tools import batch, execute_sql, last_time_breakpoint
from utils.tools import SEVEN_DAYS_OF_SECONDS
from utils.versions import version_for_timestamp

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
finished integer,
version text,
won integer,
mirror integer,
team_size integer,
game_type integer,
UNIQUE(match_id, player_id))"""

MAX_DOWNLOAD = 1000

API_TEMPLATE = "https://aoe2.net/api/matches?game=aoe2de&count={count}&since={start}"

BATCH_SIZE = 300

MAX_STARTED_DIFFERENCE = 1200

BACKWARD_JUMP = -14400  # 4 hours


def latest_version():
    """ Returns latest version available in db. """
    conn = psycopg2.connect(database="aoe2stats")
    cur = conn.cursor()
    version = 0
    for (current_version,) in cur.execute(
        "SELECT DISTINCT version FROM matches WHERE version IS NOT NULL"
    ).fetchall():
        if int(current_version) > version:
            version = int(current_version)
    conn.close()
    return version


GAME_TYPE_TO_RATING_TYPE = {
    "1v1": {0: 2, 2: 1, 12: 9, 13: 13},
    "team": {0: 4, 2: 3, 12: 9, 13: 14},
}


def one_week_ago():
    """ Returns unix timestamp of one week ago. """
    week = timedelta(days=7)
    return int(datetime.timestamp((datetime.now() - week)))


def last_match_time():
    """ Returns the time of the last match retrieved or one week ago. """
    conn = psycopg2.connect(database="aoe2stats")
    cur = conn.cursor()
    cur.execute("SELECT MAX(started) FROM matches")
    result = cur.fetchone()[0]
    conn.close()
    cutoff = one_week_ago()
    # results trickle in, so start 90 minutes before end of last run
    return result and result > cutoff and result - 5400 or cutoff


def save_matches(matches, database):
    """ Inserts each match value into the database. """
    sql = """ INSERT INTO matches
(match_id, map_type, rating_type, version, started, finished,
team_size, game_type,
player_id, civ_id, rating, won, mirror)
VALUES %s
ON CONFLICT DO NOTHING"""
    conn = psycopg2.connect(database="aoe2stats")
    cur = conn.cursor()
    for match_batch in batch(matches, BATCH_SIZE):
        cur.execute("BEGIN")

        psycopg2.extras.execute_values(cur, sql, match_batch)
        cur.execute("COMMIT")
    conn.close()


def fetch_matches(start, changeby=0):
    """ Fetches match data via one api call starting at start_time.
        start: unix timestamp to start
        changeby: for explicit jumps from start; 0 for next batch
        Returns number of matches, latest start time, and
        array of values ready for sql insert. """

    start_time = datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M")
    print("FETCHING from {} ({})".format(start_time, start))

    ranked_match_data = []
    unranked_match_data = []

    retry_strategy = Retry(backoff_factor=10, total=6)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    url = API_TEMPLATE.format(start=start, count=MAX_DOWNLOAD)
    response = http.get(url)
    if response.status_code != 200:
        print(response.text)
        sys.exit(1)
    next_start = start
    data = json.loads(response.text)
    match_ids = set()
    starteds = set()
    starteds.add(start)
    for match in data:
        # ignore if no map_type
        if not match["map_type"]:
            continue
        try:
            validate_player_info(match)
        except PlayerInfoException:
            continue

        match_rows = []

        row = [
            match["match_id"],
            match["map_type"],
            match["rating_type"],
            match["version"] or version_for_timestamp(match["started"]),
            match["started"],
            match["finished"],
            match["num_players"] / 2,
            match["game_type"],
        ]
        if match["started"] > next_start:
            next_start = match["started"]
        civs = set()
        for player in match["players"]:
            player_row = [
                player["profile_id"],
                player["civ"],
                player["rating"],
                player["won"] or False,
            ]
            match_rows.append(row + player_row)
            civs.add(player["civ"])

        # Determine if it is a mirror match (all have same civs)
        for row in match_rows:
            row.append(len(civs) == 1)

        if match["ranked"]:
            ranked_match_data += match_rows
        else:
            unranked_match_data += match_rows
        match_ids.add(match["match_id"])
        if match["started"] > start:
            starteds.add(match["started"])
    print("Match count:", len(match_ids))
    hold_started = None
    for started in sorted(starteds):
        if hold_started:
            if started - hold_started > MAX_STARTED_DIFFERENCE:
                print("JUMP OF {} SECONDS".format(started - hold_started))
                next_start = hold_started
                break
        hold_started = started
    if next_start <= start:
        print("NEXT START HAS NOT CHANGED")
        next_start += MAX_STARTED_DIFFERENCE
    if changeby:
        next_start = start + changeby
    return len(data), next_start, ranked_match_data, unranked_match_data


class PlayerInfoException(Exception):
    """ Exception thrown when match player info invalid."""


def validate_player_info(match, validate_wins=True):
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
    if validate_wins:
        if len(wins) != 2:
            raise PlayerInfoException(
                "Unreasonable number of win conditions: {}".format(len(wins))
            )
        if len(set(wins.values())) != 1:
            raise PlayerInfoException("Wins not evenly divided")


def print_time_left(script_start, fetch_start, next_start, end_ts=None):
    """ Prints info on time left. """
    now = datetime.now()
    now_ts = now.timestamp()
    time_lapsed = now_ts - script_start
    completed_coverage = float(next_start - fetch_start)

    if end_ts:
        fetch_end = end_ts
    else:
        # estimate
        fetch_end = now_ts

    total_coverage = fetch_end - fetch_start
    pct_complete = completed_coverage / total_coverage
    script_end = (time_lapsed / pct_complete) + script_start
    # recalculate if dynamic fetch_end
    if not end_ts:
        fetch_end = script_end
        total_coverage = fetch_end - fetch_start
        pct_complete = completed_coverage / total_coverage
        script_end = (time_lapsed / pct_complete) + script_start

    seconds_left = int(script_end - now_ts)
    time_remaining = timedelta(seconds=seconds_left)
    estimated_end = now + time_remaining
    print("Time Remaining: {}".format(str(time_remaining)))
    print("Estimated end: {}".format(estimated_end.strftime("%H:%M")))
    print(
        "Time left to cover:", timedelta(seconds=(int(fetch_end - next_start))),
    )


def fetch_and_save(start, end_ts):
    """ Fetches up to one week of data from start. """
    script_start = datetime.now().timestamp()
    print("Starting at {}".format(int(script_start)))
    fetch_start = start
    data_length = MAX_DOWNLOAD
    if end_ts:
        changeby = 0
        forward_start = start
    else:
        forward_start = 0
        changeby = BACKWARD_JUMP
    baseline = zero_count = 0
    for max_id, in execute_sql("SELECT MAX(id) FROM matches"):
        baseline = max_id
    count_sql = """
SELECT COUNT(DISTINCT match_id) FROM matches
WHERE id > {}""".format(baseline)
    for (cnt,) in execute_sql(count_sql):
        last_count = cnt
    while True:
        data_length, fetch_start, ranked, unranked = fetch_matches(
            fetch_start, changeby
        )
        save_matches(ranked, RANKED_DB)
        for (cnt,) in execute_sql(count_sql):
            print("Ranked match change:", cnt - last_count)
            if cnt - last_count == 0:
                zero_count += 1
            elif zero_count > 0:
                zero_count = 0
            last_count = cnt

        if zero_count > 1 and changeby == BACKWARD_JUMP:
            print(28 * "*")
            print("REVERSING...")
            print(28 * "*")
            fetch_start = fetch_start + (-3 * BACKWARD_JUMP) + 1
            forward_start = fetch_start
            changeby = 0
            script_start = datetime.now().timestamp()
        if data_length < MAX_DOWNLOAD or (end_ts and fetch_start > end_ts):
            break
        print("Next start:", fetch_start)
        print("sleeping...")
        time.sleep(5)
        if forward_start and forward_start != fetch_start:
            print_time_left(script_start, forward_start, fetch_start, end_ts)
    print("Ending at {}".format(datetime.now().strftime("%H:%M")))


def run():
    """ Parses arguments and runs the appropriate functions. """
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
    start_timestamp = None
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

    if args.lw:
        end_date = last_time_breakpoint(datetime.now())
        end_timestamp = int(end_date.timestamp())
        if not start_timestamp:
            start_timestamp = end_timestamp - SEVEN_DAYS_OF_SECONDS

    if not start_timestamp:
        start_timestamp = last_match_time()

    fetch_and_save(start_timestamp, end_timestamp)

    if args.lw:
        print("CACHING RESULTS")
        generate_results()


if __name__ == "__main__":
    run()
