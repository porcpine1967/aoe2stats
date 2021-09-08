#!/usr/bin/env python
""" Generates weekly report of map and civ usage."""
from collections import Counter, defaultdict
from datetime import datetime

from utils.tools import civ_map, execute_sql, last_time_breakpoint, timeboxes

QUERIES = {
    "match_popularity": """SELECT civ_id, COUNT(*) AS cnt FROM matches
                      WHERE civ_id IS NOT NULL AND started BETWEEN {} AND {}
                      {}
                      GROUP BY civ_id""",
    "win_rates_player": """ SELECT player_id, civ_id, won, COUNT(*) AS cnt FROM matches
                       WHERE civ_id IS NOT NULL AND started BETWEEN {} AND {}
                       AND mirror = 0
                       GROUP BY player_id, civ_id, won""",
}


class WeekInfo:
    def __init__(self):
        self.match_popularity_uses = defaultdict(Counter)
        self.match_popularity_totals = defaultdict(float)
        self.match_popularity_ranks = defaultdict(int)
        self.match_wins = defaultdict(Counter)
        self.match_losses = defaultdict(Counter)
        self.player_popularity_uses = defaultdict(Counter)
        self.player_winrates = defaultdict(list)


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

    def match_popularity_rank(self, category):
        return self.this_week.match_popularity_ranks[category]

    def match_popularity(self, category):
        this_weeks_uses = self.this_week.match_popularity_uses[category]
        this_weeks_total = self.this_week.match_popularity_totals[category]
        this_weeks_rank = self.this_week.match_popularity_ranks[category]
        last_weeks_rank = self.last_week.match_popularity_ranks[category]
        return "{:>2}. {:15} ({:+d}) ({:2.1f}%)".format(
            this_weeks_rank,
            self.name,
            last_weeks_rank - this_weeks_rank,
            100 * this_weeks_uses / this_weeks_total,
        )


def filters(category):
    """ Generates additional "where" conditions for query """
    if category == "all":
        return ""
    if category == "1v1":
        return "AND rating_type = 2"
    if category == "1v1 Arabia":
        return "AND rating_type = 2 AND map_type = 9"
    if category == "1v1 Arena":
        return "AND rating_type = 2 AND map_type = 29"


def most_popular_match(civs, week_index, timebox, category):
    """ Loads civs with most popular match for given week. """
    sql = QUERIES["match_popularity"].format(*timebox, filters(category))
    total = 0
    weeks = []
    for civ_id, count in execute_sql(sql):
        total += count
        civ = civs[civ_id]
        week = civ.week_by_index(week_index)
        weeks.append(week)
        week.match_popularity_uses[category] = count
    for rank, week in enumerate(
        sorted(weeks, reverse=True, key=lambda x: x.match_popularity_uses[category])
    ):
        week.match_popularity_ranks[category] = rank + 1
        week.match_popularity_totals[category] = total


class ReportManager:
    """ Does all the heavy lifting of making the report. """

    def __init__(self):
        self.civs = {}
        for civ_id, name in civ_map().items():
            self.civs[civ_id] = Civilization(name, civ_id)
        self.categories = (
            "1v1",
            "1v1 Arabia",
            "1v1 Arena",
        )

    def generate(self):
        """ Load data into civs. """
        last_tuesday = last_time_breakpoint(datetime.now())
        print("Report for week ending {}".format(last_tuesday.strftime("%Y-%m-%d")))
        for idx, timebox in enumerate(timeboxes(int(datetime.timestamp(last_tuesday)))):
            for category in self.categories:
                most_popular_match(self.civs, idx, timebox, category)

    def display(self):
        """ Shows report. """
        data = defaultdict(list)
        print("{:^32}     {:^32}     {:^32}".format(*self.categories))
        for category in self.categories:
            for civ in sorted(
                self.civs.values(), key=lambda x: x.match_popularity_rank(category)
            ):
                data[category].append(civ)
        for i in range(10):
            print(
                "{}     {}     {}".format(
                    *[
                        data[self.categories[j]][i].match_popularity(self.categories[j])
                        for j in range(len(self.categories))
                    ]
                )
            )


if __name__ == "__main__":
    report = ReportManager()
    report.generate()
    report.display()
