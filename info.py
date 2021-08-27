#!/usr/bin/env python
""" Basic info on the data available. """
from argparse import ArgumentParser
from collections import Counter, defaultdict
from datetime import datetime
import json

import sqlite3

from analyze import DB, latest_version, week_before_last_rating

import utils.map_pools
from utils.models import Player


def map_map():
    """ Generates dict for mapping civ id to civ name. """
    cmap = defaultdict(lambda: "UNKNOWN")
    with open("data/strings.json") as f:
        data = json.load(f)
    for civ_info in data["map_type"]:
        cmap[civ_info["id"]] = civ_info["string"]

    return cmap


def board_map():
    """ Generates dict for mapping board id to board name. """
    bmap = {}
    with open("data/strings.json") as f:
        data = json.load(f)
    for civ_info in data["rating_type"]:
        bmap[civ_info["id"]] = civ_info["string"]

    return bmap


def timestamp_to_day(timestamp):
    """ Convert timestamp into something human readable. """
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


def show_maps_player_info(where):
    """ Display table of map popularity information by player. """
    sql = """SELECT player_id, map_type, COUNT(*) FROM matches
                      WHERE {} and map_type IS NOT NULL
                      GROUP BY player_id, map_type""".format(
        " AND ".join(where)
    )

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(Player)
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_map_use(row[1], row[2])
    conn.close()
    data = Counter()
    mmap = map_map()

    for player in players.values():
        for map_type, value in player.map_preference_units.items():
            if value > 1:
                print(value)
            data[mmap[map_type]] += value
    for map_name, count in data.most_common():
        print(
            "{:30} : {:7.0f}: ({:2.0f}%)".format(
                map_name, count, (100.0 * count) / len(players)
            )
        )


def show_maps_match_info(where):
    """ Display table of map popularity information by match. """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    mmap = map_map()
    ctr = Counter()
    total = 0
    for row in cur.execute(
        """SELECT map_type, count(*) as cnt FROM matches
        WHERE {} AND map_type IS NOT NULL
        GROUP BY map_type order by cnt DESC""".format(
            " AND ".join(where)
        )
    ).fetchall():
        map_type, count = row
        ctr[mmap[map_type]] = count
        total += count
    for map_name, count in ctr.most_common():
        print(
            "{:30} : {:>7}: ({:2.0f}%)".format(map_name, count, (100.0 * count) / total)
        )
    conn.close()


def show_versions():
    """ Display a table of version:count pairs. """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    for row in cur.execute(
        """SELECT version, MIN(started), MAX(started), count(*) FROM matches
        WHERE version IS NOT NULL
        GROUP BY version"""
    ).fetchall():
        version, earliest, latest, count = row
        print(
            "{} ({} - {}): {:>7}".format(
                version, timestamp_to_day(earliest), timestamp_to_day(latest), count
            )
        )
    conn.close()


def show_boards(where):
    """ Display a table of board:count pairs."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    bmap = board_map()
    sql = """SELECT rating_type, count(*) FROM matches
        WHERE {} AND rating_type IS NOT NULL
        GROUP BY rating_type""".format(
        " AND ".join(where)
    )
    ctr = Counter()
    total = 0
    for row in cur.execute(sql).fetchall():
        rating_type, count = row
        ctr[bmap[rating_type]] = count
        total += count
    for board, count in ctr.most_common():
        print("{:30} : {:>7}: ({:2.0f}%)".format(board, count, (100.0 * count) / total))
    conn.close()


def show_pools():
    for pool_name in utils.map_pools.pools():
        print(pool_name)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--versions",
        action="store_true",
        help="What versions are available for query, with counts",
    )

    parser.add_argument(
        "--maps",
        action="store_true",
        help="What maps are available in each version, with counts",
    )

    parser.add_argument(
        "--boards", action="store_true", help="What leaderboards people are playing on",
    )

    parser.add_argument("-v", help="AOE2 Patch Version(s if comma-delimited-no-space)")

    parser.add_argument(
        "-w", action="store_true", help="Use data from just the past week"
    )

    parser.add_argument(
        "-p", action="store_true", help="Count based on player (default match)"
    )

    parser.add_argument(
        "-r", choices=("1v1", "team"), help="Limit to ranked 1v1 or team"
    )

    parser.add_argument("--pool", help="Limit to latest map pool")
    parser.add_argument("--pools", action="store_true", help="list pools")

    args = parser.parse_args()

    version = args.v or latest_version()
    where_list = ["version in ({})".format(version)]

    if args.w:
        where_list.append("started > {}".format(week_before_last_rating()))
    if args.r and args.pool:
        if args.pool == "latest":
            pool_list = utils.map_pools.latest(args.r)
        else:
            pool_list = utils.map_pools.pool(args.r, args.pool)
        where_list.append("map_type in ({})".format(pool_list))

    if args.r == "1v1":
        where_list.append("rating_type = 2")
    elif args.r == "1v1":
        where_list.append("rating_type = 4")

    if args.versions:
        show_versions()

    if args.maps:
        if args.p:
            show_maps_player_info(where_list)
        else:
            show_maps_match_info(where_list)

    if args.boards:
        show_boards(where_list)

    if args.pools:
        show_pools()
