#!/usr/bin/env python
""" Basic info on the data available. """
from argparse import ArgumentParser
from datetime import datetime
import json

import sqlite3

from analyze import DB


def map_map():
    """ Generates dict for mapping civ id to civ name. """
    cmap = {}
    with open("data/strings.json") as f:
        data = json.load(f)
    for civ_info in data["map_type"]:
        cmap[civ_info["id"]] = civ_info["string"]

    return cmap


def timestamp_to_day(timestamp):
    """ Convert timestamp into something human readable. """
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


def show_maps_info():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    mmap = map_map()
    for row in cur.execute(
        """SELECT map_type, version, count(*) FROM matches
        WHERE version IS NOT NULL AND map_type IS NOT NULL
        GROUP BY map_type, version"""
    ).fetchall():
        map_type, version, count = row
        print("{:25} : {} : {:>7}".format(mmap[map_type], version, count))
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

    args = parser.parse_args()

    if args.versions:
        show_versions()

    if args.maps:
        show_maps_info()
