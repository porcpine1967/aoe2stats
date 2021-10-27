#!/usr/bin/env python
""" Map Pool Data. """
from collections import defaultdict
from datetime import datetime, timedelta
import json
import sys
from utils.tools import (
    execute_sql,
    last_time_breakpoint,
    map_id_lookup,
    map_name_lookup,
    timeboxes,
)


RANKED_MAP_POOLS = {
    "1v1": {
        "20210427": [9, 29, 33, 74, 86, 139, 140],
        "20210504": [9, 10, 29, 71, 77, 87, 140],
        "20210518": [9, 17, 21, 29, 149, 150, 152],
        "20210601": [9, 19, 29, 33, 72, 140, 156],
        "20210615": [9, 11, 29, 32, 71, 76, 86],
        "20210629": [9, 23, 26, 29, 77, 141, 150],
        "20210713": [9, 10, 17, 29, 67, 140, 149],
        "20210727": [9, 19, 29, 72, 76, 152, 162],
        "20210810": [9, 29, 32, 77, 140, 150, 160],
        "20210824": [9, 29, 33, 67, 75, 139, 167],
        "20210907": [9, 17, 21, 29, 72, 149, 161],
        "20210921": [9, 23, 29, 71, 77, 140, 167],
        "20211006": [9, 10, 29, 32, 67, 87, 162],
        "20211020": [9, 17, 19, 29, 33, 167, 168],
    },
    "team": {
        "20210427": [9, 12, 19, 25, 29, 73, 77, 140, 141],
        "20210504": [9, 12, 17, 21, 29, 31, 33, 76, 148],
        "20210518": [9, 12, 23, 29, 77, 149, 152, 153, 155],
        "20210601": [9, 11, 12, 29, 33, 71, 72, 77, 156],
        "20210615": [9, 12, 29, 33, 74, 77, 87, 114, 140],
        "20210629": [9, 12, 19, 27, 29, 33, 77, 147, 148],
        "20210713": [9, 12, 21, 29, 31, 33, 71, 73, 77],
        "20210727": [9, 11, 12, 29, 33, 72, 77, 87, 163],
        "20210810": [9, 12, 16, 23, 29, 33, 74, 77, 161],
        "20210824": [9, 12, 29, 32, 76, 77, 149, 158, 165],
        "20210907": [9, 12, 29, 31, 33, 77, 114, 140, 166],
        "20210921": [9, 11, 12, 29, 33, 72, 74, 77, 167],
        "20211006": [9, 12, 19, 25, 29, 33, 73, 76, 77],
        "20211020": [9, 12, 23, 29, 33, 71, 72, 148, 171],
    },
}


def map_type_filter(week, size):
    """ Returns AND clause to make sure map type in a week."""
    category = "team" if size > 1 else "1v1"
    last_week = week
    for pool_week in sorted(RANKED_MAP_POOLS[category]):
        if int(pool_week) <= int(week):
            last_week = pool_week
        else:
            break
    map_pool = [str(_map) for _map in RANKED_MAP_POOLS[category][last_week]]
    return "AND map_type in ({})".format(",".join(map_pool))


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


def last_wednesday_pool(team_size=1, now=None):
    """ Using last Wednesday as a reference,
    what maps were played in ranked?"""
    _now = now or datetime.now()
    last_wednesday = last_time_breakpoint(_now)
    start = last_wednesday.strftime("%Y%m%d")
    timestamp = (last_wednesday + timedelta(days=2)).timestamp()
    lookup = map_name_lookup()
    sql = """SELECT map_type, COUNT(*) FROM matches
        WHERE started BETWEEN {:0.0f} AND {:0.0f}
        AND game_type = 0 AND team_size = {}
        GROUP BY map_type
        ORDER BY map_type""".format(
        timestamp, timestamp + 3 * 60 * 60, team_size
    )
    return '"{}": [{}],'.format(start, ", ".join([str(r) for r, _ in execute_sql(sql)]))


def run():
    mmap = map_name_lookup()
    for i in (1, 2):
        print("{}v{}".format(i, i))
        key = "1v1" if i < 2 else "team"
        last = latest(key)
        print([mmap[x] for x in last.split(",")])
        print(last)
        print(last_wednesday_pool(i))


if __name__ == "__main__":
    run()
