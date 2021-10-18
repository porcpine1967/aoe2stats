#!/usr/bin/env python
""" Runs queries and prints tables based on data drawn from aoe2.net """
from argparse import ArgumentParser
from collections import Counter, defaultdict
from datetime import datetime
import os
import statistics

import sqlite3

from utils.tools import civ_map, map_id_lookup, execute_sql
from utils.models import Player
import utils.update

DB = "data/ranked.db"


def week_before_last_rating():
    """ Returns timestamp of a week before last rating. """
    return utils.update.last_match_time() - 7 * 24 * 60 * 60


def standard_ratings(version, where=None):
    """ Returns lower and upper bounds of stdev of elo ratings.
        Assumes highest rating of a user is most accurate.
        Only really makes sense if rating_type is in query."""

    sql_template = """SELECT MAX(rating) FROM matches
                      WHERE civ_id IS NOT NULL
                      AND rating IS NOT NULL
                      AND version in ({}){}
                      GROUP BY player_id"""
    sql = sql_from_template(sql_template, version, where)

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    elos = [x[0] for x in cur.execute(sql).fetchall()]
    conn.close()
    if not elos:
        return 0, 0
    mean = statistics.mean(elos)
    std = statistics.pstdev(elos, mean)
    return mean - std, mean + std


def most_popular_player(version, where=None):
    """ Returns number of players and an array of
        civ name:player-preference-units in order of descending popularity."""

    sql_template = """SELECT player_id, civ_id, COUNT(*) FROM matches
                      WHERE civ_id IS NOT NULL AND version in ({}){}
                      GROUP BY player_id, civ_id"""
    sql = sql_from_template(sql_template, version, where)

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(Player)
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_civ_use(row[1], row[2])
    conn.close()
    data = Counter()
    cmap = civ_map()
    for player in players.values():
        for civ_id, value in player.civ_preference_units.items():
            data[cmap[civ_id]] += value
    return len(players), data


def most_popular_match(version, where=None):
    """ Returns an array of civ name:number of plays in order of descending
popularity."""
    sql_template = """SELECT civ_id, COUNT(*) as cnt FROM matches
                      WHERE civ_id IS NOT NULL AND version in ({}){}
                      GROUP BY civ_id"""
    sql = sql_from_template(sql_template, version, where)

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
    sql_template = """ SELECT player_id, civ_id, won, COUNT(*) as cnt FROM matches
                       WHERE civ_id IS NOT NULL AND version in ({}){}
                       AND mirror = 0
                       GROUP BY player_id, civ_id, won"""
    sql = sql_from_template(sql_template, version, where)

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    players = defaultdict(Player)
    for row in cur.execute(sql).fetchall():
        players[row[0]].add_civ_win(row[1], row[2], row[3])
    conn.close()
    total_win_rate(players)
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
            continue
        data[cmap[civ_id]] = statistics.mean(percentages)
    return len(players), data


def total_win_rate(players):
    """ Prints out total win percentage of players in sample. """
    win_counter = Counter()
    win_percentages = []
    for player in players.values():
        wins = 0
        losses = 0
        for win_type, count in player.wins.items():
            win_counter[win_type] += count
            if win_type:
                wins = count
            else:
                losses = count
        win_percentages.append(100.0 * wins / (wins + losses))
    print(
        "Pct wins in group (player): {:2.0f}%".format(statistics.mean(win_percentages))
    )
    print(
        "Pct wins in group (match): {:2.0f}%".format(
            100.0 * win_counter[1] / (win_counter[0] + win_counter[1])
        )
    )


def win_rates_match(version, where=None):
    """ Returns an array of civ name: win percentage in order of
        descending wins. """
    sql_template = """ SELECT civ_id, won, COUNT(*) as cnt FROM matches
                       WHERE civ_id IS NOT NULL AND version in ({}){}
                       AND mirror = 0
                       GROUP BY civ_id, won"""
    sql = sql_from_template(sql_template, version, where)

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


def sql_from_template(template, version, where_array):
    """ Formats template with standard variables."""
    return template.format(version, where_array_to_sql(where_array))


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


def display(metric, version, where, cap):
    """ Print table of data. """
    if metric == "popularity":
        civ_data = collate_popularities(version, where)
    elif metric == "winrate":
        civ_data = collate_win_rates(version, where)

    print("Civilization   : Player :  Match : Diff")

    def by_player_rank(civ_data_kv_pair):
        return civ_data_kv_pair[1]["player_rank"]

    civ_data_by_player_rank = sorted(civ_data.items(), key=by_player_rank)

    for idx, civ_info in enumerate(civ_data_by_player_rank):
        if cap and idx >= cap:
            break

        civilization_name, data = civ_info

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
    map_ids = map_id_lookup()

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
    parser.add_argument("-m", choices=map_ids.keys(), help="Which map")
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
    parser.add_argument(
        "-noelo", action="store_true", help="Data from players with no elo"
    )
    parser.add_argument("-n", type=int, help="Max number of civs to show")

    parser.add_argument(
        "-no-unrated",
        action="store_true",
        help="Remove matches where one or both players have no elo",
    )
    parser.add_argument(
        "-unrated",
        action="store_true",
        help="Matches where one or both players have no elo",
    )
    args = parser.parse_args()
    version = args.v or latest_version()
    where = []
    if args.query == "1v1":
        where.append("game_type = 0 AND team_size = 1")
    elif args.query == "team":
        where.append("game_type = 0 AND team_size > 1")

    if args.m:
        where.append("map_type = {}".format(map_ids[args.m]))

    if args.w:
        where.append("started > {}".format(week_before_last_rating()))

    if args.noelo:
        where.append("rating IS NULL")
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

    if args.unrated:
        where.append(
            "match_id IN (SELECT DISTINCT match_id FROM matches WHERE rating IS NULL)"
        )
    if args.no_unrated:
        where.append(
            "match_id NOT IN (SELECT match_id FROM matches WHERE rating IS NULL)"
        )
    cap = args.n or 0

    display(args.metric, version, where, cap)


def missing_days():
    """ Print ranges with more than 1 day between matches."""
    sql = "SELECT DISTINCT started from matches ORDER BY started"
    last_match_time = None
    for (started,) in execute_sql(sql):
        if last_match_time:
            if started - last_match_time > 86400:
                d = datetime.fromtimestamp(last_match_time)
                print("{:>6} seconds from {}".format(started - last_match_time, d))
                cmd = " python utils/update.py --start-ts {} --end-ts {}".format(
                    last_match_time, started
                )
                print(cmd)
                os.system(cmd)
        last_match_time = started
    os.system("python utils/update.py")
    os.system("python utils/results_cacher.py")


if __name__ == "__main__":
    missing_days()
