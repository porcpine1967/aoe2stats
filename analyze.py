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
        self.civ_wins = defaultdict(dict)
        self.total = 0.0

    def add_civ_use(self, civ, civ_count):
        """ Adds civilization usage data for later calculations. """
        self.civ_uses[civ] += civ_count
        self.total += civ_count

    def add_civ_win(self, civ, won, win_count):
        """ Adds civilization win data for later calculations. """
        self.civ_wins[civ][won] = win_count

    @property
    def preference_units(self):
        """ Generates preference units of player. """
        pus = {}
        for civ, total in self.civ_uses.items():
            pus[civ] = total / self.total
        return pus

    def win_percentage(self, civ):
        """ Calculates the user's win percentage for the given civ."""
        data = self.civ_wins[civ]
        if 1 in data and 0 not in data:
            return 1
        if 1 not in data and 0 in data:
            return 0
        win_count = data[1]
        total = data[0] + data[1]
        return float(win_count) / total


def standard_ratings(version, where=None):
    """ Returns lower and upper bounds of stdev of elo ratings.
        Assumes highest rating of a user is most accurate.
        Only really makes sense of rating_type is in query."""

    sql = """SELECT MAX(rating) FROM matches
            WHERE civ_id IS NOT NULL AND rating IS NOT NULL AND version in ({}){}
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
            WHERE civ_id IS NOT NULL AND version in ({}){}
            GROUP BY player_id, civ_id""".format(
        version, where_array_to_sql(where)
    )
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(Player)
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_civ_use(row[1], row[2])
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
            WHERE civ_id IS NOT NULL AND version in ({}){}
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


def win_rates_player(version, where=None):
    """ Returns an array of civ name: win percentage in order of
descending wins. """
    sql = """ SELECT player_id, civ_id, won, COUNT(*) as cnt FROM matches
              WHERE civ_id IS NOT NULL AND version in ({}){}
              AND mirror = 0
              GROUP BY player_id, civ_id, won""".format(
        version, where_array_to_sql(where)
    )
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(Player)
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_civ_win(row[1], row[2], row[3])
    conn.close()
    cmap = civ_map()
    win_calculator = defaultdict(list)
    for civ_id in cmap:
        for player in players.values():
            try:
                win_calculator[civ_id].append(player.win_percentage(civ_id))
            except KeyError:
                pass
    data = Counter()
    for civ_id, percentages in win_calculator.items():
        if not percentages:
            print(civ_id)
        data[cmap[civ_id]] = statistics.mean(percentages)
    return len(players), data


def win_rates_match(version, where=None):
    """ Returns an array of civ name: win percentage in order of
descending wins. """
    sql = """ SELECT civ_id, won, COUNT(*) as cnt FROM matches
              WHERE civ_id IS NOT NULL AND version in ({}){}
              AND mirror = 0
              GROUP BY civ_id, won""".format(
        version, where_array_to_sql(where)
    )

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    civs = defaultdict(dict)
    total = 0
    for row in cur.execute(sql).fetchall():
        civ, won, count = row
        total += count
        civs[civ][won] = count
    conn.close()

    cmap = civ_map()
    civ_percentages = Counter()
    for civ, data in civs.items():
        civ_percentages[cmap[civ]] = data[1] / sum([data[0], data[1]])
    return total, civ_percentages


def where_array_to_sql(where):
    """ Turns a set of where clauses into an sql string. """
    if not where:
        return ""
    return "".join([" AND {}".format(x) for x in where])


def collate_win_rates(version, where=None):
    """ Returns map of civilizations to map of popularity attributes """

    civ_data = defaultdict(dict)
    player_total, player_popularity = win_rates_player(version, where)
    match_total, match_popularity = win_rates_match(version, where)
    print("Total players: {:7d}".format(int(player_total)))
    print("Total matches: {:7d}".format(int(match_total)))
    rank = 0
    for civ_name, total in match_popularity.most_common():
        rank += 1
        civ_info = civ_data[civ_name]
        civ_info["match_rank"] = rank
        civ_info["match_score"] = total
    rank = 0
    for civ_name, total in player_popularity.most_common():
        rank += 1
        civ_info = civ_data[civ_name]
        civ_info["player_rank"] = rank
        civ_info["player_score"] = total

    return civ_data


def collate_popularities(version, where=None):
    """ Returns map of civilizations to map of popularity attributes """

    civ_data = defaultdict(dict)
    player_total, player_popularity = most_popular_player(version, where)
    match_total, match_popularity = most_popular_match(version, where)

    print("Total players: {:7d}".format(int(player_total)))
    print("Total matches: {:7d}".format(int(match_total)))
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
    print("Version:", version)
    return version


def display(metric, version, where):
    """ Print table of data. """
    if metric == "popularity":
        civ_data = collate_popularities(version, where)
    elif metric == "winrate":
        civ_data = collate_win_rates(version, where)

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
    parser.add_argument(
        "metric", choices=("popularity", "winrate",), help="Which metric to show"
    )
    parser.add_argument("-m", choices=("arabia", "arena",), help="Which map")
    parser.add_argument(
        "-v", default=None, help="AOE2 Patch Version(s if comma-delimited-no-space)"
    )
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
    version = args.v or latest_version()
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
        low, _ = standard_ratings(version, where)
        print("All elos below {}".format(int(low)))
        where.append("rating < {}".format(low))
    elif args.midelo:
        low, high = standard_ratings(version, where)
        print("All elos between {} and {}".format(int(low), int(high)))
        where.append("rating BETWEEN {} AND {}".format(low, high))
    elif args.highelo:
        _, high = standard_ratings(version, where)
        print("All elos above {}".format(int(high)))
        where.append("rating > {}".format(high))

    display(args.metric, version, where)


if __name__ == "__main__":
    run()
