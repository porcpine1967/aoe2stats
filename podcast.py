#!/usr/bin/env python
""" Info for podcast."""
from datetime import datetime, timedelta
from utils.tools import civ_map, execute_sql, last_time_breakpoint


class Rank:
    def __init__(self, row, week, metric, category):
        self.week = week
        self.metric = metric
        self.category = category
        self.civ_id = row[0]
        self.rank = row[1]
        self.weeks_top_5 = 0
        self.weeks_this_year = 0
        self._set_data()

    def __str__(self):
        return "{:>2}: {:11} ({:>2} weeks above {}, {:>2} weeks top 5)".format(
            self.rank,
            civ_map()[self.civ_id],
            self.weeks_this_year,
            self.rank,
            self.weeks_top_5,
        )

    def _set_data(self):
        """ set weeks at rank and previous civ """

        sql = """SELECT COUNT(*) FROM results
WHERE rank <= {}
AND civ_id = {}
AND metric = "{}"
AND team_size = 1
AND methodology = "player"
AND map_category = "{}"
GROUP BY civ_id""".format(
            self.rank, self.civ_id, self.metric, self.category
        )
        for (count,) in execute_sql(sql):
            self.weeks_this_year = count
        sql = """SELECT COUNT(*) FROM results
WHERE rank <= 5
AND civ_id = {}
AND metric = "{}"
AND team_size = 1
AND methodology = "player"
AND map_category = "{}"
AND week > "20210101"
GROUP BY civ_id""".format(
            self.civ_id, self.metric, self.category
        )
        for (count,) in execute_sql(sql):
            self.weeks_top_5 = count


def run():
    """ Flow control"""
    last_tuesday = last_time_breakpoint(datetime.now())
    this_week = (last_tuesday - timedelta(days=7)).strftime("%Y%m%d")
    print(this_week)
    for metric in ("popularity", "winrate"):
        print("*" * len(metric))
        print(metric)
        print("*" * len(metric))
        for category in ("All", "Arabia", "Arena", "Others"):
            sql = """SELECT civ_id, rank FROM results
WHERE rank < 6
and metric = "{}"
and team_size = 1
and methodology = "player"
and map_category = "{}"
and week = {}""".format(
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
