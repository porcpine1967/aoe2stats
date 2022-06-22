#!/usr/bin/env python
""" Map Pool Data. """
from collections import defaultdict
from datetime import datetime, timedelta, timezone
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
        "20210428": [9, 29, 33, 74, 86, 139, 140],
        "20210505": [9, 10, 29, 71, 77, 87, 140],
        "20210519": [9, 17, 21, 29, 149, 150, 152],
        "20210602": [9, 19, 29, 33, 72, 140, 156],
        "20210616": [9, 11, 29, 32, 71, 76, 86],
        "20210630": [9, 23, 26, 29, 77, 141, 150],
        "20210714": [9, 10, 17, 29, 67, 140, 149],
        "20210728": [9, 19, 29, 72, 76, 152, 162],
        "20210811": [9, 29, 32, 77, 140, 150, 160],
        "20210825": [9, 29, 33, 67, 75, 139, 167],
        "20210908": [9, 17, 21, 29, 72, 149, 161],
        "20210922": [9, 23, 29, 71, 77, 140, 167],
        "20211006": [9, 10, 29, 32, 67, 87, 162],
        "20211020": [9, 17, 19, 29, 33, 167, 168],
        "20211103": [9, 29, 71, 72, 76, 87, 140],
        "20211117": [9, 29, 77, 149, 150, 165, 167],
        "20211201": [9, 16, 21, 29, 140, 167, 170],
        "20211215": [9, 23, 29, 71, 141, 167, 172],
        "20220112": [33, 29, 9, 77, 140, 72, 87],
        "20220126": [9, 149, 10, 29, 27, 174, 17],
        "20220209": [167, 9, 69, 67, 29, 77, 116],
        "20220223": [167, 29, 9, 33, 87, 140, 170],
        "20220323": [9, 29, 149, 77, 167, 72, 19],
        "20220504": [9, 150, 73, 71, 140, 29, 23],
        "20220518": [33, 9, 72, 29, 171, 77, 21],
        "20220601": [141, 29, 9, 140, 87, 10, 172],
        "20220615": [9, 29, 77, 165, 149, 86, 72],
    },
    "team": {
        "20210428": [9, 12, 19, 25, 29, 73, 77, 140, 141],
        "20210505": [9, 12, 17, 21, 29, 31, 33, 76, 148],
        "20210519": [9, 12, 23, 29, 77, 149, 152, 153, 155],
        "20210602": [9, 11, 12, 29, 33, 71, 72, 77, 156],
        "20210616": [9, 12, 29, 33, 74, 77, 87, 114, 140],
        "20210630": [9, 12, 19, 27, 29, 33, 77, 147, 148],
        "20210714": [9, 12, 21, 29, 31, 33, 71, 73, 77],
        "20210728": [9, 11, 12, 29, 33, 72, 77, 87, 163],
        "20210811": [9, 12, 16, 23, 29, 33, 74, 77, 161],
        "20210825": [9, 12, 29, 32, 76, 77, 149, 158, 165],
        "20210908": [9, 12, 29, 31, 33, 77, 114, 140, 166],
        "20210922": [9, 11, 12, 29, 33, 72, 74, 77, 167],
        "20211006": [9, 12, 19, 25, 29, 33, 73, 76, 77],
        "20211020": [9, 12, 23, 29, 33, 71, 72, 148, 171],
        "20211103": [9, 12, 29, 32, 33, 67, 73, 74, 77],
        "20211117": [9, 12, 16, 21, 29, 31, 33, 77, 167],
        "20211201": [9, 12, 17, 29, 33, 77, 141, 147, 165],
        "20211215": [9, 11, 12, 29, 33, 74, 77, 164, 167],
        "20220112": [149, 9, 29, 77, 33, 12, 72, 71, 19],
        "20220126": [73, 9, 31, 12, 77, 148, 29, 33, 23],
        "20220209": [9, 33, 74, 75, 29, 77, 12, 167, 114],
        "20220223": [29, 12, 33, 11, 72, 9, 77, 25, 17],
        "20220323": [33, 9, 31, 29, 19, 12, 77, 167, 16],
        "20220504": [29, 33, 12, 140, 85, 9, 155, 77, 71],
        "20220518": [147, 149, 29, 33, 12, 23, 9, 74, 170],
        "20220601": [17, 9, 29, 77, 12, 33, 31, 165, 72],
        "20220615": [29, 33, 9, 87, 169, 75, 12, 78, 77],
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


def current_pool(team_size=1):
    """ Determines current pool and cutoff date for adding to RANKED_MAP_POOLS"""
    pool_key = "team" if team_size > 1 else "1v1"
    last_pool_s = max(RANKED_MAP_POOLS[pool_key].keys())
    last_cutoff = datetime(
        int(last_pool_s[:4]),
        int(last_pool_s[4:6]),
        int(last_pool_s[6:]),
        1,
        tzinfo=timezone.utc,
    ).timestamp()
    last_wednesday = last_time_breakpoint(datetime.now()) + timedelta(hours=6)
    last_wednesday_ts = last_wednesday.timestamp()

    lookup = map_name_lookup()
    sql = """SELECT map_type, max(started) as s
             FROM matches
             WHERE started > {:0.0f}
             AND game_type = 0 AND team_size = {}
             GROUP BY map_type
             ORDER BY s DESC""".format(
        last_cutoff, team_size
    )
    week_key = ""
    ids = []
    names = []
    last_start = 0
    for map_type, start in execute_sql(sql):
        print(lookup[map_type], map_type, datetime.utcfromtimestamp(start))
        if start < last_wednesday_ts:
            break_day = datetime.utcfromtimestamp(last_start)
            week_key = break_day.strftime("%Y%m%d")
            break
        last_start = start
        ids.append(str(map_type))
        names.append(lookup[map_type])
    if not week_key or week_key == last_pool_s:
        return "{} up to date".format(pool_key)
    returnables = []
    returnables.append("KEY: {}".format(pool_key))
    returnables.append('"{}": [{}],'.format(week_key, ", ".join(ids)))
    returnables.append("({})".format(", ".join(names)))
    return "\n".join(returnables)


def run():
    """ Do what is necessary."""
    for size in (
        1,
        2,
    ):
        print(current_pool(size))
        print("")


if __name__ == "__main__":
    run()
