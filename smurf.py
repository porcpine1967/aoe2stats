#!/usr/bin/env python
""" Finds the new users with big jumps in elo """
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

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
SELECT player_id, COUNT(*) as cnt, MAX(rating) AS max_rating,
MIN(started) AS min_started, MIN(rating) AS min_rating
FROM matches
WHERE civ_id IS NOT NULL
AND game_type = 0 AND team_size = 1
AND started < {:0.0f}
GROUP BY player_id
HAVING min(started) > {:0.0f} and COUNT(*) > 5 and max(rating) > 1699
"""

WEEK_IN_SECONDS = 7 * 24 * 60 * 60
COUNTRIES = country_map()

RATING_CUTOFF = 1700


class Match:
    """ Holder of match info."""

    def __init__(self, row):
        self.rating = row[0] or 0
        self.civ_id = row[1]
        self.map_type = row[2]
        self.started = row[3]
        self.won = row[4]


def sort_started(match):
    """ Function for sorting matches by started."""
    return match.started


class WeekInfo:
    def __init__(self, start, matches, end_cutoff):
        self.start = start
        end = start + WEEK_IN_SECONDS
        matches = [match for match in matches if start < match.started < end]
        self.max_rating = max([match.rating for match in matches])
        self.matches = []
        self.start_time = None
        self.end_time = None
        for match in sorted(matches, key=sort_started):
            if match.started < end_cutoff and match.rating > RATING_CUTOFF:
                self.end_time = datetime.fromtimestamp(match.started, tz=timezone.utc)
                break

            if not self.start_time:
                self.start_time = datetime.utcfromtimestamp(match.started)
            self.matches.append(match)
            if match.rating == self.max_rating:
                self.end_time = datetime.utcfromtimestamp(match.started)
                break

    @property
    def win_pct(self):
        return (
            1.0
            * len([match for match in self.matches if match.won])
            / len(self.matches)
        )

    @property
    def diff(self):
        ratings = [match.rating for match in self.matches if match.rating]
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

    def valid(self, end_cutoff):
        return (
            self.games_played > 5
            and self.end_time.timestamp() > end_cutoff
            and self.max_rating > RATING_CUTOFF
        )

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

    def __init__(self, row, timebox):
        start_cutoff, end_cutoff = timebox
        self.player_id = row[0]
        self.validate(start_cutoff)
        if not self.valid:
            return
        week_info = self._best_week(end_cutoff)
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
            try:
                if data["country"] in COUNTRIES:
                    self.country = COUNTRIES[data["country"]]
                else:
                    self.country = data["country"]
            except KeyError:
                self.country = ""
        else:
            self.valid = False

    def validate(self, start_cutoff):
        sql = """
SELECT min(started)
FROM matches
WHERE player_id = {}""".format(self.player_id)
        for started, in execute_sql(sql):
            self.valid = started >= start_cutoff

    def _best_week(self, end_cutoff):
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
            matches.append(Match(row))
        week_info = WeekInfo(matches[0].started, matches, end_cutoff)
        if week_info.valid(end_cutoff):
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


def display(date_reference):
    """ Returns smurfs from past week."""
    wednesday = last_time_breakpoint(date_reference).timestamp()
    last_week, _ = timeboxes(wednesday)
    sql = SQL.format(wednesday, last_week[0])
    smurfs = []
    for row in execute_sql(sql):
        smurf = Smurf(row, last_week)
        if smurf.valid:
            smurfs.append(smurf)
    for smurf in sorted(smurfs, key=lambda x: x.max_rating, reverse=True):
        print(smurf)

def run():
    """ Flow control function."""
    parser = ArgumentParser()
    _ = parser.parse_args()
    display(datetime.now())

if __name__ == "__main__":
    run()
