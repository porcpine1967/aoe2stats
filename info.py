#!/usr/bin/env python
""" Basic info on the data available. """
from argparse import ArgumentParser
from datetime import datetime

import sqlite3

from analyze import DB


def timestamp_to_day(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


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

    args = parser.parse_args()

    if args.versions:
        show_versions()
