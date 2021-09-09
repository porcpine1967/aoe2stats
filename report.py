#!/usr/bin/env python
""" Generates weekly report of map and civ usage."""
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
import statistics

from utils.models import Player
from utils.tools import civ_map, execute_sql, last_time_breakpoint, timeboxes

QUERIES = {
    "match_popularity": """SELECT civ_id, COUNT(*) AS cnt FROM matches
                      WHERE civ_id IS NOT NULL
                      AND started BETWEEN {:0.0f} AND {:0.0f}
                      {}
                      GROUP BY civ_id""",
    "player_popularity": """SELECT player_id, civ_id, COUNT(*) AS cnt FROM matches
                      WHERE civ_id IS NOT NULL
                      AND started BETWEEN {:0.0f} AND {:0.0f}
                      {}
                      GROUP BY player_id, civ_id""",
    "win_rates_match": """ SELECT civ_id, won, COUNT(*) as cnt FROM matches
                           WHERE civ_id IS NOT NULL
                           AND mirror = 0
                           AND started BETWEEN {:0.0f} AND {:0.0f}
                           {}
                           GROUP BY civ_id, won""",
    "win_rates_player": """ SELECT player_id, civ_id, won, COUNT(*) AS cnt FROM matches
                            WHERE civ_id IS NOT NULL
                            AND mirror = 0
                            AND started BETWEEN {:0.0f} AND {:0.0f}
                            {}
                            GROUP BY player_id, civ_id, won""",
}


class WeekInfo:
    """ Holder for weekly civ data. """

    def __init__(self):
        self.popularity_uses = defaultdict(float)
        self.popularity_totals = defaultdict(float)
        self.popularity_ranks = defaultdict(int)
        self.win_results = defaultdict(list)
        self.win_ranks = defaultdict(int)

    def popularity_rank(self, category):
        """ Rank in category on match popularity metric. """
        return self.popularity_ranks[category]

    def popularity_pct(self, category):
        """ Percentage of plays in this category this week. """
        this_weeks_uses = self.popularity_uses[category]
        this_weeks_total = self.popularity_totals[category] or 1
        return 100 * this_weeks_uses / this_weeks_total

    def winrate_rank(self, category):
        """ Rank of civ for this week in terms of win rate."""
        return self.win_ranks[category]

    def winrate_pct(self, category):
        """ Percentage of games won by this week in this category. """
        if not self.win_results[category]:
            return 0
        return 100 * statistics.mean(self.win_results[category])


class Civilization:
    """ Holds information per civ. """

    def __init__(self, name, civ_id):
        self.civ_id = civ_id
        self.name = name
        self.last_week = WeekInfo()
        self.this_week = WeekInfo()

    def week_by_index(self, index):
        """ Returns week by index for dynamic choosing.
        0 == last week; 1 == this week."""
        if index == 0:
            return self.last_week
        return self.this_week

    def rank(self, metric, category):
        """ Returns rank of civ this week in terms of match popularity, for sorting """
        if metric == "popularity":
            return self.this_week.popularity_rank(category)
        elif metric == "winrate":
            return self.this_week.winrate_rank(category)
        return 0

    def info(self, metric, category):
        """ String representation of match popularity of civ in given category. """
        if metric == "popularity":
            this_weeks_rank = self.this_week.popularity_rank(category)
            last_weeks_rank = self.last_week.popularity_rank(category)
            pct = self.this_week.popularity_pct(category)
        elif metric == "winrate":
            this_weeks_rank = self.this_week.winrate_rank(category)
            last_weeks_rank = self.last_week.winrate_rank(category)
            pct = self.this_week.winrate_pct(category)
        else:
            return ""
        return "{:>2}. {:15} ({:+3d}) ({:2.1f}%)".format(
            this_weeks_rank, self.name, last_weeks_rank - this_weeks_rank, pct,
        )


def filters(category):
    """ Generates additional "where" conditions for query """
    if category == "1v1":
        return "AND rating_type = 2"
    if category == "1v1 Arabia":
        return "AND rating_type = 2 AND map_type = 9"
    if category == "1v1 Arena":
        return "AND rating_type = 2 AND map_type = 29"
    return ""


def most_popular_match(civs, week_index, timebox, category):
    """ Loads civs with most popular by match for given week. """
    sql = QUERIES["match_popularity"].format(*timebox, filters(category))
    total = 0
    weeks = []
    for civ_id, count in execute_sql(sql):
        total += count
        civ = civs[civ_id]
        week = civ.week_by_index(week_index)
        weeks.append(week)
        week.popularity_uses[category] += count

    def week_sorter(week):
        return -1 * week.popularity_uses[category]

    for rank, week in enumerate(sorted(weeks, key=week_sorter), 1):
        week.popularity_ranks[category] = rank
        week.popularity_totals[category] = total


def most_popular_player(civs, week_index, timebox, category):
    """ Loads civs with most popular by player for given week. """
    sql = QUERIES["player_popularity"].format(*timebox, filters(category))
    players = defaultdict(Player)
    for player_id, civ_id, count in execute_sql(sql):
        players[player_id].add_civ_use(civ_id, count)

    weeks = set()
    for player in players.values():
        for civ_id, value in player.civ_preference_units.items():
            week = civs[civ_id].week_by_index(week_index)
            weeks.add(week)
            week.popularity_uses[category] += value

    def week_sorter(week):
        return -1 * week.popularity_uses[category]

    for rank, week in enumerate(sorted(weeks, key=week_sorter), 1):
        week.popularity_ranks[category] = rank
        week.popularity_totals[category] = len(players)


def winrate_match(civs, week_index, timebox, category):
    """ Loads civs with winrate data based on matches. """
    sql = QUERIES["win_rates_match"].format(*timebox, filters(category))
    weeks = set()
    for civ_id, won, count in execute_sql(sql):
        week = civs[civ_id].week_by_index(week_index)
        weeks.add(week)
        week.win_results[category] += [won for _ in range(count)]

    def week_sorter(week):
        return -1 * week.winrate_pct(category)

    for rank, week in enumerate(sorted(weeks, key=week_sorter), 1):
        week.win_ranks[category] = rank


def winrate_player(civs, week_index, timebox, category):
    """ Loads civs with winrate data based on player percentage. """
    sql = QUERIES["win_rates_player"].format(*timebox, filters(category))
    players = defaultdict(Player)
    for player_id, civ_id, won, count in execute_sql(sql):
        players[player_id].add_civ_win(civ_id, won, count)

    weeks = set()
    for civ_id, civ in civs.items():
        week = civ.week_by_index(week_index)
        weeks.add(week)
        for player in players.values():
            try:
                week.win_results[category].append(player.win_percentage(civ_id))
            except KeyError:
                pass

    def week_sorter(week):
        return -1 * week.winrate_pct(category)

    for rank, week in enumerate(sorted(weeks, key=week_sorter), 1):
        week.win_ranks[category] = rank


class ReportManager:
    """ Does all the heavy lifting of making the report. """

    def __init__(self, args):
        self.args = args
        self.civs = {}
        self.report_types = set()
        if args.w:
            self.report_types.add("winrate")
        elif args.p:
            self.report_types.add("popularity")
        else:
            self.report_types.update(("winrate", "popularity",))
        for civ_id, name in civ_map().items():
            self.civs[civ_id] = Civilization(name, civ_id)
        self.categories = (
            "1v1",
            "1v1 Arabia",
            "1v1 Arena",
        )

    def generate(self):
        """ Load data into civs. Returns report date."""
        if self.args.m:
            methodology = "match"
        else:
            methodology = "player"

        last_tuesday = last_time_breakpoint(datetime.now())
        for idx, timebox in enumerate(timeboxes(datetime.timestamp(last_tuesday))):
            for category in self.categories:
                if methodology == "player":
                    most_popular_player(self.civs, idx, timebox, category)
                    winrate_player(self.civs, idx, timebox, category)
                elif methodology == "match":
                    most_popular_match(self.civs, idx, timebox, category)
                    winrate_match(self.civs, idx, timebox, category)
        return last_tuesday

    def display(self, report_date):
        """ Shows report. """
        message = "Report for week ending {}".format(report_date.strftime("%Y-%m-%d"))
        print("*" * len(message))
        print(message)
        print("*" * len(message))
        for report_type in self.report_types:
            print("")
            print(report_type.capitalize())
            data = defaultdict(list)
            print(
                "    ".join(["{:^33}" for _ in range(len(self.categories))]).format(
                    *self.categories
                )
            )
            for category in self.categories:

                def civ_sorter(civ):
                    return civ.rank(report_type, category)

                for civ in sorted(self.civs.values(), key=civ_sorter):
                    data[category].append(civ)
            for i in range(self.args.n or len(self.civs)):
                print(
                    "    ".join(["{}" for _ in range(len(self.categories))]).format(
                        *[
                            data[self.categories[j]][i].info(
                                report_type, self.categories[j]
                            )
                            for j in range(len(self.categories))
                        ]
                    )
                )


def run():
    """ Basic functioning of app. Removes global variables. """
    parser = ArgumentParser()
    parser.add_argument("-w", action="store_true", help="Only winrates")
    parser.add_argument("-p", action="store_true", help="Only popularity")
    parser.add_argument("-m", action="store_true", help="Use match methodology")
    parser.add_argument("-n", type=int, help="Only show n records")
    args = parser.parse_args()
    report = ReportManager(args)
    generation_enddate = report.generate()
    report.display(generation_enddate)


if __name__ == "__main__":
    run()
