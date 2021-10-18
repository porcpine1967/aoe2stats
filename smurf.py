#!/usr/bin/env python
""" Finds the new users with big jumps in elo """
from argparse import ArgumentParser
from datetime import datetime

from utils.tools import (
    civ_map,
    execute_sql,
    map_name_lookup,
    last_time_breakpoint,
    timeboxes,
)


SQL = """
SELECT player_id, avg(won), COUNT(*) as cnt, max(rating) as max_rating,
min(started) as min_started, max(rating) - min(rating) as change,
GROUP_CONCAT(DISTINCT map_type), GROUP_CONCAT(DISTINCT civ_id)
FROM matches
WHERE civ_id IS NOT NULL
AND game_type = 0 AND team_size = 1
AND rating is not null
and started < {:0.0f}
GROUP BY player_id
HAVING min_started > {:0.0f} and cnt > 5 and max_rating > 1600
ORDER BY change DESC
LIMIT 10
"""


class Smurf:
    """ Data holder for smurf-like player"""

    def __init__(self, row):
        self.player_id = row[0]
        self.win_pct = row[1]
        self.games_played = row[2]
        self.max_rating = row[3]
        self.diff = row[5]
        self.map_types = row[6].split(",")
        self.civ_ids = row[7].split(",")

    @property
    def civs(self):
        """ Names of civs used by smurf."""
        return [civ_map()[civ_id] for civ_id in self.civ_ids]

    @property
    def maps(self):
        """ Names of civs used by smurf."""
        return sorted([map_name_lookup()[civ_id] for civ_id in self.map_types])

    def __str__(self):
        if len(self.map_types) < 3:
            maps = "({})".format(", ".join(self.maps))
        else:
            maps = ""
        if len(self.civ_ids) < 3:
            civs = "({})".format(", ".join(self.civs))
        else:
            civs = ""
        return """Player {}:
   Win Pct: {:10.0f}%
   Games played: {:5}
   Max Rating: {:7}
   Change: {:11}
   Number maps: {:6}  {}
   Number civs: {:6}  {}
""".format(
            self.player_id,
            100 * self.win_pct,
            self.games_played,
            self.max_rating,
            self.diff,
            len(self.map_types),
            maps,
            len(self.civ_ids),
            civs,
        )


def smurfs():
    """ Returns smurfs from past week."""
    tuesday = last_time_breakpoint(datetime(2021, 10, 8)).timestamp()
    last_week, this_week = timeboxes(tuesday)
    sql = SQL.format(this_week[-1], last_week[0])
    for row in execute_sql(sql):
        print(Smurf(row))


def run():
    """ Flow control function."""
    parser = ArgumentParser()
    _ = parser.parse_args()
    smurfs()


if __name__ == "__main__":
    run()
