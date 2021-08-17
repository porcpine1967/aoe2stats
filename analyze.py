#!/usr/bin/env python
""" Runs queries and prints tables based on data drawn from aoe2.net """
from collections import Counter, defaultdict
import json

import sqlite3

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


def civ_map():
    """ Generates dict for mapping civ id to civ name. """
    cmap = {}
    with open("data/strings.json") as f:
        data = json.load(f)
    for civ_info in data["civ"]:
        cmap[civ_info["id"]] = civ_info["string"]

    return cmap


def most_popular_player(where=""):
    """ Returns an array of civ name:player-preference-units in order of descending
popularity."""
    sql = """SELECT player_id, civ_id, COUNT(*) FROM matches
             {}
             GROUP BY player_id, civ_id""".format(
        where
    )
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(lambda: Player())
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_civ(row[1], row[2])

    data = Counter()
    cmap = civ_map()
    for player in players.values():
        for civ_id, value in player.preference_units.items():
            data[cmap[civ_id]] += value
    return data


def most_popular_match(where=""):
    """ Returns an array of civ name:number of plays in order of descending
popularity."""
    sql = """SELECT civ_id, COUNT(*) as cnt FROM matches
            {}
            GROUP BY civ_id""".format(
        where
    )

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    data = Counter()
    cmap = civ_map()
    for row in cur.execute(sql).fetchall():
        data[cmap[row[0]]] = row[1]
    return data


def collate_popularities():
    """ Returns map of civilizations to map of popularity attributes """

    civ_data = defaultdict(lambda: {})
    player_popularity = most_popular_player()
    match_popularity = most_popular_match()

    rank = 0
    for civ_name, total in player_popularity.most_common():
        rank += 1
        civ_info = civ_data[civ_name]
        civ_info["player_rank"] = rank
        civ_info["player_score"] = total

    rank = 0
    for civ_name, total in match_popularity.most_common():
        rank += 1
        civ_info = civ_data[civ_name]
        civ_info["match_rank"] = rank
        civ_info["match_score"] = total

    return civ_data


def run():
    """ Make it easy to switch ad hoc what one does. """
    civ_data = collate_popularities()
    for civ_info in sorted(civ_data.items(), key=lambda x: x[1]["player_rank"]):
        print(
            "{:15}: {:5.0f}: {:5.0f}".format(
                civ_info[0], civ_info[1]["player_score"], civ_info[1]["match_score"],
            )
        )


if __name__ == "__main__":
    run()
