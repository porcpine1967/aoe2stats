#!/usr/bin/env python
""" Generates weekly report of map and civ usage."""
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timedelta

from utils.tools import civ_map, execute_sql, last_time_breakpoint

QUERIES = {
    "popularity": """SELECT civ_id, pct, rank FROM results
                           WHERE week = "{}"
                           {}
                           AND methodology = "{}"
                           AND metric = "popularity"
                           AND compound = {}
    """,
    "win_rates": """SELECT civ_id, pct, rank FROM results
                           WHERE week = "{}"
                           {}
                           AND methodology = "{}"
                           AND metric = "winrate"
    """,
}


class CivDict(defaultdict):
    """ Generates civ if missing."""

    def __init__(self):
        super().__init__()
        self.cmap = civ_map()

    def __missing__(self, key):
        str_key = str(key)
        if str_key in self:
            return self[str_key]
        civ_ids = []
        civ_names = []

        for i in str_key.split(":"):
            civ_ids.append(i)
            civ_names.append(self.cmap[int(i)])
            self[str_key] = Civilization(":".join(civ_names), ":".join(civ_ids))
        return self[str_key]


def build_category_filters(start, end):
    """ Generate category_filters because easier."""
    category_filters = {}
    for i in range(start, end + 1):
        for category in (
            "All",
            "Arabia",
            "Arena",
            "Others",
        ):
            category_filters[
                "{}v{} {}".format(i, i, category)
            ] = "AND map_category = '{}' AND team_size = {}".format(category, i)
    return category_filters


CATEGORY_FILTERS = build_category_filters(1, 4)


class WeekInfo:
    """ Holder for weekly civ data. """

    def __init__(self):
        self.popularity_pcts = defaultdict(float)
        self.popularity_ranks = defaultdict(int)
        self.winrate_ranks = defaultdict(int)
        self.winrate_pcts = {}

    def popularity_rank(self, category):
        """ Rank in category on match popularity metric. """
        return self.popularity_ranks[category]

    def popularity_pct(self, category):
        """ Percentage of plays in this category this week. """
        return 100 * self.popularity_pcts[category]

    def winrate_rank(self, category):
        """ Rank of civ for this week in terms of win rate."""
        return self.winrate_ranks[category]

    def winrate_pct(self, category):
        """ Percentage of games won by this week in this category. """
        return 100 * self.winrate_pcts[category]


def info_template(num_civs):
    """ Generates template based on number of civs."""
    civ_length = 11 * num_civs + num_civs - 1
    return "{{:>2}}. {{:{}}} ({{:+4d}}) ({{:4.1f}}%)".format(civ_length)


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
        """ Returns rank of civ this week."""
        if metric == "popularity":
            return self.this_week.popularity_rank(category)
        if metric == "winrate":
            return self.this_week.winrate_rank(category)
        return 0

    def info(self, metric, category, num_civs):
        """ String representation of civ in given category. """
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
        return info_template(num_civs).format(
            this_weeks_rank, self.name, last_weeks_rank - this_weeks_rank, pct,
        )


def filters(category):
    """ Generates additional "where" conditions for query """
    return CATEGORY_FILTERS[category]


def most_popular(civs, week_index, week, category, methodology, compound):
    """ Loads civs with most popular by match for given week. """
    sql = QUERIES["popularity"].format(week, filters(category), methodology, compound)
    for civ_id, pct, rank in execute_sql(sql):
        civ = civs[civ_id]
        week = civ.week_by_index(week_index)
        week.popularity_pcts[category] = pct
        week.popularity_ranks[category] = rank


def winrate(civs, week_index, week, category, methodology):
    """ Loads civs with winrate data based on matches. """
    sql = QUERIES["win_rates"].format(week, filters(category), methodology)
    for civ_id, pct, rank in execute_sql(sql):
        civ = civs[civ_id]
        week = civ.week_by_index(week_index)
        week.winrate_pcts[category] = pct
        week.winrate_ranks[category] = rank


class ReportManager:
    """ Does all the heavy lifting of making the report. """

    def __init__(self, args):
        self.args = args
        self.civs = CivDict()
        if args.w:
            self.report_types = ("winrate",)
        elif args.p:
            self.report_types = ("popularity",)
        else:
            self.report_types = (
                "popularity",
                "winrate",
            )
        self.categories = list(build_category_filters(args.s, args.s).keys())

    def generate(self, endtime=datetime.now()):
        """ Load data into civs. Returns report date."""
        if self.args.m:
            methodology = "match"
        else:
            methodology = "player"

        last_tuesday = last_time_breakpoint(endtime)
        last_week = (last_tuesday - timedelta(days=14)).strftime("%Y%m%d")
        this_week = (last_tuesday - timedelta(days=7)).strftime("%Y%m%d")
        for idx, timebox in enumerate((last_week, this_week,)):
            for category in self.categories:
                if not self.args.w:
                    most_popular(
                        self.civs, idx, timebox, category, methodology, self.args.c
                    )
                if not self.args.p:
                    winrate(self.civs, idx, timebox, category, methodology)
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
            template = "{:^" + str(18 + 12 * self.args.s) + "}"
            print(
                "    ".join([template for _ in range(len(self.categories))]).format(
                    *self.categories
                )
            )
            for category in self.categories:

                def civ_sorter(civ):
                    return civ.rank(report_type, category)

                for civ in sorted(self.civs.values(), key=civ_sorter):
                    if civ.rank(report_type, category):
                        data[category].append(civ)

            data_category_length = min([len(civs) for civs in data.values()])
            for i in range(self.args.n or data_category_length):
                print(
                    "    ".join(["{}" for _ in range(len(self.categories))]).format(
                        *[
                            data[self.categories[j]][i].info(
                                report_type, self.categories[j], self.args.s
                            )
                            for j in range(len(self.categories))
                        ]
                    )
                )


def arg_parser():
    """ args returned from command line."""
    parser = ArgumentParser()
    parser.add_argument("-w", action="store_true", help="Only winrates")
    parser.add_argument("-p", action="store_true", help="Only popularity")
    parser.add_argument("-m", action="store_true", help="Use match methodology")
    parser.add_argument("-n", type=int, help="Only show n records")
    parser.add_argument("-s", default=1, type=int, help="Team size")
    parser.add_argument("-c", action="store_true", help="Use compound report for team")
    return parser


def run():
    """ Basic functioning of app. Removes global variables. """
    args = arg_parser().parse_args()
    report = ReportManager(args)
    generation_enddate = report.generate()
    report.display(generation_enddate)


if __name__ == "__main__":
    run()
