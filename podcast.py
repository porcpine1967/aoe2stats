#!/usr/bin/env python
""" Info for podcast."""
from datetime import datetime, timedelta

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import TransferManager

from map_report import run as run_map_report
from utils.tools import civ_map, execute_sql, last_time_breakpoint


class Rank:
    def __init__(self, row, week, metric, category):
        self.week = week
        self.metric = metric
        self.category = category
        self.civ_id = row[0]
        self.rank = row[1]
        self.weeks_at_level = 0
        self.weeks_top_5 = 0
        self.weeks_this_year = 0
        self._set_data()

    def __str__(self):
        return "{:>2}: {:11} ({:>2} weeks at/above {}, {:>2} weeks top 5, {:2} weeks top 5 this year)".format(
            self.rank,
            civ_map()[self.civ_id],
            self.weeks_at_level,
            self.rank,
            self.weeks_top_5,
            self.weeks_this_year,
        )

    def _set_data(self):
        """set weeks at rank and previous civ"""

        now = datetime.strptime(self.week, "%Y%m%d")
        sql = """SELECT week FROM results
WHERE rank > {}
AND civ_id = '{}'
AND metric = '{}'
AND team_size = '1v1'
AND methodology = 'player'
AND map_category = '{}'
and week < '{}'
and week != '2021'
ORDER BY week DESC
LIMIT 1""".format(
            self.rank, self.civ_id, self.metric, self.category, self.week
        )

        last_year_dt = now - timedelta(days=365)
        last_year = last_year_dt.strftime("%Y%m%d")

        for (week,) in execute_sql(sql):
            then = datetime.strptime(week, "%Y%m%d")
            self.weeks_at_level = int((now - then).days / 7)
        sql = """SELECT week FROM results
WHERE rank <= 5
AND civ_id = '{}'
AND metric = '{}'
AND team_size = '1v1'
AND methodology = 'player'
AND map_category = '{}'
AND week > '{}'
AND week <= '{}'
ORDER BY week DESC""".format(
            self.civ_id, self.metric, self.category, last_year, self.week
        )
        continuous_week_ctr = 0
        week_ctr = 0
        last_week = now
        continuous = True
        for (week,) in execute_sql(sql):
            week_ctr += 1
            this_week = datetime.strptime(week, "%Y%m%d")
            if continuous and (last_week - this_week).days <= 7:
                continuous_week_ctr += 1
                last_week = this_week
            else:
                continuous = False
        self.weeks_top_5 = continuous_week_ctr
        self.weeks_this_year = week_ctr

def run_transfer_report():
    manager = TransferManager(Loader())
    transfers = manager.recent_transfers()
    good_transfers = []
    for transfer in transfers:
        if transfer.old != transfer.new:
            good_transfers.append(transfer)
    if good_transfers:
        print("****************************")
        print("TRANSFERS")
        print("****************************")
        for transfer in good_transfers:
            print("{}: From: {:20} To: {:20} {:20} ({})".format(transfer.date,
                                                       transfer.old or '',
                                                       transfer.new or '',
                                                       transfer.players[0][0],
                                                                transfer.ref or ''))
            for player in transfer.players[1:]:
                print(" "*64 + player[0])
            print()


def run():
    """Flow control"""
    last_wednesday = last_time_breakpoint(datetime.now())
    this_week = (last_wednesday - timedelta(days=7)).strftime("%Y%m%d")
    print(this_week)
    for metric in ("popularity", "winrate",):
        print("*" * len(metric))
        print(metric)
        print("*" * len(metric))
        for category in ("All", "Arabia", "Arena"):
            if metric == "winrate" and category == "All":
                continue
            sql = """SELECT civ_id, rank FROM results
WHERE rank < 9
and metric = '{}'
and team_size = '1v1'
and methodology = 'player'
and map_category = '{}'
and week = '{}'""".format(
                metric, category, this_week
            )
            print("*" * 28)
            print(category)
            ranks = []
            for row in execute_sql(sql):
                ranks.append(Rank(row, this_week, metric, category))
            for rank in sorted(ranks, key=lambda x: x.rank):
                print(rank)


if __name__ == "__main__":
    run()
    print("****************************")
    print("MAP REPORT")
    print("****************************")
    run_map_report()
    print("****************************")
    run_transfer_report()
