#!/usr/bin/env python
""" Finds the new users with big jumps in elo """
from argparse import ArgumentParser
from datetime import datetime, timedelta

from utils.tools import (
    civ_map,
    country_map,
    execute_sql,
    map_name_lookup,
    last_time_breakpoint,
    timeboxes,
    user_info,
)


SQL = """
SELECT player_id, COUNT(*) as cnt, max(rating) as max_rating,
min(started) as min_started, min(rating) as min_rating
FROM matches
WHERE civ_id IS NOT NULL
AND game_type = 0 AND team_size = 1
GROUP BY player_id
HAVING min_started > {:0.0f} and cnt > 5 and max_rating > 1699 and min_rating < 1500
"""

WEEK_IN_SECONDS = 7 * 24 * 60 * 60
COUNTRIES = country_map()


class Match:
    """ Holder of match info."""

    def __init__(self, row):
        self.rating = row[0]
        self.civ_id = row[1]
        self.map_type = row[2]
        self.started = row[3]
        self.won = row[4]


class WeekInfo:
    def __init__(self, start, matches):
        self.start = start
        self.end = start + WEEK_IN_SECONDS
        self.matches = [
            match
            for match in matches
            if match.started > start and match.started < self.end
        ]

    @property
    def win_pct(self):
        return (
            1.0
            * len([match for match in self.matches if match.won])
            / len(self.matches)
        )

    @property
    def max_rating(self):
        return max([match.rating for match in self.matches])

    @property
    def diff(self):
        ratings = [match.rating for match in self.matches]
        return max(ratings) - min(ratings)

    @property
    def map_types(self):
        return {match.map_type for match in self.matches}

    @property
    def civ_ids(self):
        return {match.civ_id for match in self.matches}

    @property
    def games_played(self):
        return len(self.matches)

    @property
    def start_time(self):
        return datetime.fromtimestamp(min([match.started for match in self.matches]))

    @property
    def end_time(self):
        return datetime.fromtimestamp(max([match.started for match in self.matches]))

    def __str__(self):
        return "\n   ".join(
            [
                "{} - {}".format(self.start_time, self.end_time),
                str(self.max_rating),
                str(self.diff),
                str(self.games_played),
                str(self.win_pct),
            ]
        )


class Smurf:
    """ Data holder for smurf-like player"""

    def __init__(self, row):
        self.player_id = row[0]
        week_info = self._best_week()
        if week_info:
            self.valid = True
            self.timebox = "{} - {}".format(
                week_info.start_time.strftime("%Y-%m-%d"),
                week_info.end_time.strftime("%Y-%m-%d"),
            )
            self.max_rating = week_info.max_rating
            self.games_played = week_info.games_played
            self.win_pct = week_info.win_pct
            self.diff = week_info.diff
            self.map_types = week_info.map_types
            self.civ_ids = week_info.civ_ids
            data = user_info(self.player_id)
            self.username = data["name"]
            if data["country"] in COUNTRIES:
                self.country = COUNTRIES[data["country"]]
            else:
                self.country = data["country"]
        else:
            self.valid = False

    def _best_week(self):
        sql = """
SELECT rating, civ_id, map_type, started, won
FROM matches
WHERE civ_id IS NOT NULL
AND game_type = 0 AND team_size = 1
AND player_id = {}
ORDER BY started
        """.format(
            self.player_id
        )
        matches = []
        for row in execute_sql(sql):
            if row[0]:
                matches.append(Match(row))
        week_info = WeekInfo(matches[0].started, matches)
        if week_info.games_played > 5:
            return week_info
        return None

    @property
    def civs(self):
        """ Names of civs used by smurf."""
        return [civ_map()[civ_id] for civ_id in self.civ_ids]

    @property
    def maps(self):
        """ Names of civs used by smurf."""
        return sorted([map_name_lookup()[civ_id] for civ_id in self.map_types])

    def __str__(self):
        if not self.valid:
            return "(INVALID)"
        if len(self.map_types) < 3:
            maps = "({})".format(", ".join(self.maps))
        else:
            maps = ""
        if len(self.civ_ids) < 8:
            civs = "({})".format(", ".join(self.civs))
        else:
            civs = ""
        return """{} ({}): https://aoe2.net/#profile-{}
   Timebox: {}
   Max Rating: {:7}
   Win Pct: {:10.0f}%
   Games played: {:5}
   Change: {:11}
   Number maps: {:6}  {}
   Number civs: {:6}  {}
""".format(
            self.username,
            self.country,
            self.player_id,
            self.timebox,
            self.max_rating,
            100 * self.win_pct,
            self.games_played,
            self.diff,
            len(self.map_types),
            maps,
            len(self.civ_ids),
            civs,
        )


def display():
    """ Returns smurfs from past week."""
    wednesday = last_time_breakpoint(datetime.now()).timestamp()
    last_week, _ = timeboxes(wednesday)
    sql = SQL.format(last_week[0])
    smurfs = []
    for row in execute_sql(sql):
        smurf = Smurf(row)
        if smurf.valid:
            smurfs.append(smurf)
    for smurf in sorted(smurfs, key=lambda x: x.max_rating, reverse=True):
        print(smurf)


def run():
    """ Flow control function."""
    parser = ArgumentParser()
    _ = parser.parse_args()
    display()


if __name__ == "__main__":
    run()
