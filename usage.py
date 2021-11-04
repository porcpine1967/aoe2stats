#!/usr/bin/env python
""" Weekend Usage statistics.
Friday - Monday
Unique players
Number of matches
- 1v1
- team
Ranked and unranked
"""
from collections import defaultdict
from datetime import datetime, timedelta
from utils.tools import execute_sql, weekend

SQLS = {
    "Matches": """SELECT COUNT(DISTINCT match_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}""",
    "Players": """SELECT COUNT(DISTINCT player_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}""",
    "1v1 Ma": """SELECT COUNT(DISTINCT match_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND team_size = 1""",
    "1v1 Pl": """SELECT COUNT(DISTINCT player_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND team_size = 1""",
    "Team Ma": """SELECT COUNT(DISTINCT match_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND team_size > 1""",
    "Team Pl": """SELECT COUNT(DISTINCT player_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND team_size > 1""",
    "1v1 M H": """SELECT COUNT(DISTINCT match_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND rating > 1650
AND team_size = 1""",
    "1v1 P H": """SELECT COUNT(DISTINCT player_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND rating > 1650
AND team_size = 1""",
    "1v1 M M": """SELECT COUNT(DISTINCT match_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND rating BETWEEN 1000 AND 1650
AND team_size = 1""",
    "1v1 P M": """SELECT COUNT(DISTINCT player_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND rating BETWEEN 1000 AND 1650
AND team_size = 1""",
    "1v1 M L": """SELECT COUNT(DISTINCT match_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND rating > 1000
AND team_size = 1""",
    "1v1 P L": """SELECT COUNT(DISTINCT player_id) FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
AND rating < 1000
AND team_size = 1""",
}

TEMPLATE = "{:14}" + "{:>9}" * len(SQLS)


def run():
    """ Start analysis."""
    keys = list(SQLS.keys())
    for mode in (
        "ranked",
        "unranked",
    ):
        print("\n" + mode.capitalize())
        print(TEMPLATE.format("Week", *keys))

        for week_offset in range(4):
            data = {}
            for label, sql_template in SQLS.items():
                timebox = weekend(datetime.utcnow() - timedelta(weeks=week_offset))
                dates = "{}:{}".format(
                    datetime.utcfromtimestamp(timebox[0]).strftime("%m-%d"),
                    datetime.utcfromtimestamp(timebox[1]).strftime("%m-%d"),
                )
                sql = sql_template.format(*timebox)
                for (cnt,) in execute_sql(sql, "data/{}.db".format(mode)):
                    data[label] = cnt
            print(TEMPLATE.format(dates, *[data[key] for key in keys]))


if __name__ == "__main__":
    run()
