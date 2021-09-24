#!/usr/bin/env python
""" Map Pool Data. """
from collections import defaultdict
from datetime import datetime, timedelta
import json
import sys
from utils.tools import execute_sql, last_time_breakpoint, timeboxes


RANKED_MAP_POOLS = {
    "1v1": {
        "20201215": [67, 9, 29, 140, 32, 71, 116],
        "20210112": [9, 29, 72, 33, 75, 23, 76],
        "20210126": [149, 9, 10, 29, 150, 148, 151],
        "20210727": [9, 29, 152, 72, 19, 162, 76],
        "20210803": [152, 162, 19, 29, 72, 76, 9],
        "20210810": [9, 29, 150, 140, 32, 160, 77],
        "20210824": [67, 9, 29, 139, 167, 33, 75],
        "20210907": [161, 9, 149, 72, 17, 29, 21],
        "20210921": [9, 167, 77, 140, 71, 29],
    },
    "team": {
        "20201215": [9, 29, 12, 72, 77, 33, 23, 76, 27],
        "20210112": [9, 29, 12, 17, 141, 74, 114, 25, 87],
        "20210126": [149, 9, 29, 150, 11, 12, 148, 77, 151],
        "20210727": [9, 29, 11, 12, 72, 163, 77, 33, 87],
        "20210803": [11, 12, 163, 29, 33, 72, 77, 87, 9],
        "20210810": [9, 29, 12, 16, 74, 161, 77, 33, 23],
        "20210824": [149, 9, 29, 12, 158, 32, 77, 165, 76],
        "20210907": [114, 12, 140, 166, 29, 31, 33, 77, 9],
        "20210921": [11, 12, 167, 29, 33, 72, 74, 77, 9],
    },
}


def map_id_lookup():
    """ Returns a dictionary of map_name:map_id pairs. """
    mmap = defaultdict(lambda: "UNKNOWN")
    with open("data/strings.json") as open_file:
        data = json.load(open_file)
    for civ_info in data["map_type"]:
        mmap[civ_info["string"]] = civ_info["id"]
        mmap[civ_info["string"].lower()] = civ_info["id"]

    return mmap


def map_name_lookup():
    """ Returns a dictionary of map_id:map_name pairs. """
    mmap = defaultdict(lambda: "UNKNOWN")
    with open("data/strings.json") as open_file:
        data = json.load(open_file)
    for civ_info in data["map_type"]:
        mmap[civ_info["id"]] = civ_info["string"]

    return mmap


def ids_from_names(names):
    """ Prints list of map type ids from list of names. """
    mmap = map_id_lookup()
    print([mmap[name] for name in names])


def latest(key):
    """ Returns list of map types in most recent map pool for 1v1 or team. """
    latest_map_types = []
    latest_pool = ""
    for pool_name, map_types in RANKED_MAP_POOLS[key].items():
        if pool_name > latest_pool:
            latest_pool = pool_name
            latest_map_types = map_types
    print("Latest Pool: {}".format(latest_pool))
    return ",".join([str(i) for i in latest_map_types])


def pools():
    """ Returns all pools in ascending order. """
    pool_names = set()
    for _, pool_tuple in RANKED_MAP_POOLS.items():
        pool_names.update(pool_tuple)
    return sorted(list(pool_names))


def pool(rating, pool_name):
    """ Returns the list of map_types for a given pool. """
    return ",".join([str(i) for i in RANKED_MAP_POOLS[rating][pool_name]])


def last_wednesday_pool(now=None):
    """ Using last Wednesday as a reference,
    what maps were played in ranked?"""
    _now = now or datetime.now()
    last_tuesday = last_time_breakpoint(_now)
    print(last_tuesday)
    start = last_tuesday.strftime("%Y%m%d")
    timestamp = (last_tuesday + timedelta(days=1)).timestamp()
    lookup = map_name_lookup()
    for team_size in (1, 2, 3, 4):
        sql = """SELECT map_type, COUNT(*) FROM matches
        WHERE started BETWEEN {:0.0f} AND {:0.0f}
        AND game_type = 0 AND team_size = {}
        GROUP BY map_type""".format(
            timestamp - 10 * 60 * 60, timestamp + 2 * 60 * 60, team_size
        )
        print("{}v{}".format(team_size, team_size))
        print(
            ", ".join(
                sorted(
                    ["{} ({}:{})".format(lookup[r], r, c) for r, c in execute_sql(sql)]
                )
            )
        )
        print(
            '"{}": [{}],'.format(
                start, ", ".join(sorted([str(r) for r, _ in execute_sql(sql)])),
            )
        )


if __name__ == "__main__":

    last_wednesday_pool(datetime(2021, 7, 28))
