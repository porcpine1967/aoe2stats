#!/usr/bin/env python
""" Runs queries and prints tables based on data drawn from aoe2.net """
from argparse import ArgumentParser
from collections import Counter, defaultdict
import json
import statistics

import sqlite3

import utils.update

DB = "data/aoe2.net.db"


class Player:
    """ Object to calculate player-preference-units. """

    def __init__(self):
        self.civ_uses = Counter()
        self.total = 0.0

    def add_civ(self, civ, civ_count):
        """ Adds civilization data for later calculations. """
        self.civ_uses[civ] += civ_count
        self.total += civ_count

    @property
    def preference_units(self):
        """ Generates preference units of player. """
        pus = {}
        for civ, total in self.civ_uses.items():
            pus[civ] = total / self.total
        return pus

    def rating(self, rating_type):
        """ Returns the highest rating at that rating type."""


def standard_ratings(version, where=None):
    """ Returns lower and upper bounds of stdev of elo ratings.
        Assumes highest rating of a user is most accurate.
        Only really makes sense of rating_type is in query."""

    sql = """SELECT MAX(rating) FROM matches
            WHERE civ_id IS NOT NULL AND rating IS NOT NULL AND version = {}{}
            GROUP BY player_id""".format(
        version, where_array_to_sql(where)
    )

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    elos = [x[0] for x in cur.execute(sql).fetchall()]
    conn.close()
    mean = statistics.mean(elos)
    std = statistics.pstdev(elos, mean)
    return mean - std, mean + std


def civ_map():
    """ Generates dict for mapping civ id to civ name. """
    cmap = {}
    with open("data/strings.json") as f:
        data = json.load(f)
    for civ_info in data["civ"]:
        cmap[civ_info["id"]] = civ_info["string"]

    return cmap


def most_popular_player(version, where=None):
    """ Returns number of players and an array of
        civ name:player-preference-units in order of descending popularity."""

    sql = """SELECT player_id, civ_id, COUNT(*) FROM matches
            WHERE civ_id IS NOT NULL AND version = {}{}
            GROUP BY player_id, civ_id""".format(
        version, where_array_to_sql(where)
    )
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(Player)
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_civ(row[1], row[2])
    conn.close()
    data = Counter()
    cmap = civ_map()
    for player in players.values():
        for civ_id, value in player.preference_units.items():
            data[cmap[civ_id]] += value
    return len(players), data


def most_popular_match(version, where=None):
    """ Returns an array of civ name:number of plays in order of descending
popularity."""
    sql = """SELECT civ_id, COUNT(*) as cnt FROM matches
            WHERE civ_id IS NOT NULL AND version = {}{}
            GROUP BY civ_id""".format(
        version, where_array_to_sql(where)
    )

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    data = Counter()
    cmap = civ_map()
    total = 0.0
    for row in cur.execute(sql).fetchall():
        total += row[1]
        data[cmap[row[0]]] = row[1]
    conn.close()
    return total, data


def where_array_to_sql(where):
    """ Turns a set of where clauses into an sql string. """
    if not where:
        return ""
    return "".join([" AND {}".format(x) for x in where])


def collate_popularities(version, where=None):
    """ Returns map of civilizations to map of popularity attributes """

    civ_data = defaultdict(dict)
    player_total, player_popularity = most_popular_player(version, where)
    match_total, match_popularity = most_popular_match(version, where)

    print("Total matches: {:7d}".format(int(match_total)))
    print("Total players: {:7d}".format(int(player_total)))
    rank = 0
    for civ_name, total in player_popularity.most_common():
        rank += 1
        civ_info = civ_data[civ_name]
        civ_info["player_rank"] = rank
        civ_info["player_absolute_score"] = total
        civ_info["player_score"] = total / player_total

    rank = 0
    for civ_name, total in match_popularity.most_common():
        rank += 1
        civ_info = civ_data[civ_name]
        civ_info["match_rank"] = rank
        civ_info["match_absolute_score"] = total
        civ_info["match_score"] = total / match_total

    return civ_data


def latest_version():
    """ Returns latest version available in db. """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    version = 0
    for row in cur.execute("SELECT DISTINCT version FROM matches").fetchall():
        current_version = row[0]
        if not current_version:
            continue
        if int(current_version) > version:
            version = int(current_version)
    conn.close()
    return version


def analyze_popularity(version=None, where=None):
    """ Print out results of popularity analysis """

    civ_data = collate_popularities(version, where)
    print("Civilization   : Player :  Match : Diff")
    for idx, civ_info in enumerate(
        sorted(civ_data.items(), key=lambda x: x[1]["player_rank"])
    ):

        civilization_name = civ_info[0]
        data = civ_info[1]
        rank_diff = data["player_rank"] - data["match_rank"]
        if rank_diff == 0:
            rank_diff = ""
        else:
            rank_diff = "{:+}".format(rank_diff)
        print(
            "{:>2}. {:11}: {:5.1f}% : {:5.1f}% : {}".format(
                idx + 1,
                civilization_name,
                data["player_score"] * 100,
                data["match_score"] * 100,
                rank_diff,
            )
        )


def run():
    """ Make it easy to switch ad hoc what one does. """
    parser = ArgumentParser()
    parser.add_argument(
        "query",
        default="all",
        choices=("all", "1v1", "team",),
        help="Which records to analyze",
    )
    parser.add_argument("-m", choices=("arabia", "arena",), help="Which map")
    parser.add_argument("-v", default=None, help="AOE2 Patch Version")
    parser.add_argument(
        "-w", action="store_true", help="Use data from just the past week"
    )
    parser.add_argument("-gelo", type=int, help="Elo greater than")
    parser.add_argument("-lelo", type=int, help="Elo less than")
    parser.add_argument(
        "-lowelo", action="store_true", help="Data from elos below stdev"
    )
    parser.add_argument(
        "-midelo", action="store_true", help="Data from elos within stdev"
    )
    parser.add_argument(
        "-highelo", action="store_true", help="Data from elos above stdev"
    )
    args = parser.parse_args()
    where = []
    if args.query == "1v1":
        where.append("rating_type = 2")
    elif args.query == "team":
        where.append("rating_type = 4")

    if args.m == "arabia":
        where.append("map_type = 9")
    elif args.m == "arena":
        where.append("map_type = 29")

    if args.w:
        where.append("started > {}".format(utils.update.one_week_ago()))

    if args.gelo:
        where.append("rating > {}".format(args.gelo))
    elif args.lelo:
        where.append("rating < {}".format(args.lelo))
    elif args.lowelo:
        low, _ = standard_ratings(args.v or latest_version(), where)
        print("All elos below {}".format(int(low)))
        where.append("rating < {}".format(low))
    elif args.midelo:
        low, high = standard_ratings(args.v or latest_version(), where)
        print("All elos between {} and {}".format(int(low), int(high)))
        where.append("rating BETWEEN {} AND {}".format(low, high))
    elif args.highelo:
        _, high = standard_ratings(args.v or latest_version(), where)
        print("All elos above {}".format(int(high)))
        where.append("rating > {}".format(high))

    analyze_popularity(args.v or latest_version(), where)


if __name__ == "__main__":
    run()
