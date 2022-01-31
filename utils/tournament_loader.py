#!/usr/bin/env python3
""" Fetches data from db and/or liquipedia"""
from argparse import ArgumentParser

from datetime import datetime

import psycopg2

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import PlayerManager, TournamentManager

from utils.tools import execute_sql, tournament_timeboxes

DEBUG = False

GAMES = ("Age of Empires II", "Age of Empires IV",)
TIERS = ("S-Tier", "A-Tier", "B-Tier",)

FROM_SQL = """
SELECT tier, start_date, end_date, prize, participant_count,
first_place, first_place_url, second_place, description,
series, organizers, sponsors, game_mode, format, team, runners_up, name
FROM tournaments
WHERE url = '{}'
"""

INSERT_SQL = """
INSERT INTO tournaments
(name, url, game, tier, start_date, end_date, prize, participant_count, first_place, first_place_url, second_place, description, series, organizers, sponsors, game_mode, format, team, runners_up)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

UPDATE_SQL = """
UPDATE tournaments
SET
name=%s,
tier=%s,
start_date=%s,
end_date=%s,
prize=%s,
participant_count=%s,
first_place=%s,
first_place_url=%s,
second_place=%s,
description=%s,
series=%s,
organizers=%s,
sponsors=%s,
game_mode=%s,
format=%s,
team=%s,
runners_up=%s
WHERE url = %s
"""

PLAYER_RESULTS_COUNT_SQL = """
SELECT COUNT(*) FROM player_results
WHERE player_url = '{}'
AND tournament_url = '{}'
"""

TOURNAMENT_COUNT_SQL = """
SELECT COUNT(*) FROM tournaments
WHERE url = '{}'
"""

SAVE_PLAYER_RESULTS_SQL = """
INSERT INTO player_results
(player_url, player_place, player_prize, tournament_url)
VALUES (%s, %s, %s, %s)
ON CONFLICT (player_url, tournament_url) DO UPDATE SET
player_place=Excluded.player_place,
player_prize=Excluded.player_prize
"""

PLAYER_RESULTS_SQL = """
SELECT tournaments.name,
tournaments.game,
tournaments.tier,
results.player_place,
tournaments.end_date,
tournaments.first_place,
tournaments.team
FROM tournaments
JOIN
player_results AS results ON tournaments.url = results.tournament_url
WHERE
results.player_url = '{}'
AND
tournaments.tier IN ('{}', '{}', '{}')
AND
tournaments.game IN ('{}', '{}')
AND tournaments.end_date > current_date - interval '1 year'
ORDER BY
tournaments.end_date DESC
"""

SEP = ", "

class TournamentLoader:
    def __init__(self):
        self.tournament_manager = TournamentManager(Loader())
        self.loader = self.tournament_manager.loader

    def starting(self, timebox):
        """ Fetch information on all upcoming tournaments."""
        return self._tournament_dict(self.tournament_manager.starting(timebox).items())

    def ongoing(self, timebox):
        """ Fetch information on all upcoming tournaments."""
        return self._tournament_dict(self.tournament_manager.ongoing(timebox).items())

    def completed(self, timebox):
        """ Fetch information on all upcoming tournaments."""
        return self._tournament_dict(self.tournament_manager.completed(timebox).items())

    def _tournament_dict(self, items):
        tournament_dict = {}
        for game, tournaments in items:
            if not game in GAMES:
                continue
            tournament_dict[game] = self._db_tournaments(tournaments)
        return tournament_dict
        
    def _db_tournaments(self, tournaments):
        db_tournaments = []
        for api_tournament in tournaments:
            if not api_tournament.tier in TIERS:
                continue
            tournament = Tournament(api_tournament, self.loader)
            db_tournaments.append(tournament)
        return db_tournaments

def execute_transaction(sql, values):
    conn = psycopg2.connect(database="aoe2stats")
    cur = conn.cursor()
    cur.execute("BEGIN")

    cur.execute(sql, values)
    cur.execute("COMMIT")
    
def save_tournament(api_tournament):
    """ Persists api_tournament to db."""
    row = (
        api_tournament.name,
        api_tournament.url,
        api_tournament.game,
        api_tournament.tier,
        api_tournament.start,
        api_tournament.end,
        api_tournament.prize,
        api_tournament.participant_count,
        api_tournament.first_place,
        api_tournament.first_place_url,
        api_tournament.second_place,
        api_tournament.description,
        api_tournament.series,
        SEP.join(api_tournament.organizers),
        SEP.join(api_tournament.sponsors),
        api_tournament.game_mode,
        api_tournament.format_style,
        api_tournament.team,
        SEP.join(api_tournament.runners_up),
    )
    execute_transaction(INSERT_SQL, row)

def update_tournament(api_tournament):
    row = (
        api_tournament.name,
        api_tournament.tier,
        api_tournament.start,
        api_tournament.end,
        api_tournament.prize,
        api_tournament.participant_count,
        api_tournament.first_place,
        api_tournament.first_place_url,
        api_tournament.second_place,
        api_tournament.description,
        api_tournament.series,
        SEP.join(api_tournament.organizers),
        SEP.join(api_tournament.sponsors),
        api_tournament.game_mode,
        api_tournament.format_style,
        api_tournament.team,
        SEP.join(api_tournament.runners_up),
        api_tournament.url,
    )
    execute_transaction(UPDATE_SQL, row)

def save_player(player_url, loader):
    player_manager = PlayerManager(loader)
    for api_tournament in player_manager.tournaments(player_url):
        row = (player_url,
               api_tournament.loader_place,
               api_tournament.loader_prize,
               api_tournament.url,)
        execute_transaction(SAVE_PLAYER_RESULTS_SQL, row)
        # Only save basic attributes; no updating
        for count, in execute_sql(TOURNAMENT_COUNT_SQL.format(api_tournament.url)):
            if count < 1:
                save_tournament(api_tournament)

class Tournament:
    def __init__(self, api_tournament, loader):
        self.api_tournament = api_tournament
        self.url = api_tournament.url
        self._load(loader)
        
    def _load(self, loader):
        sql = FROM_SQL.format(self.api_tournament.url)
        in_db = False
        for row in execute_sql(sql):
            in_db = True
            self.tier = row[0]
            self.start = row[1]
            self.end = row[2]
            self.prize = row[3]
            self.participant_count = row[4]
            self.first_place = row[5]
            self.first_place_url = row[6]
            self.second_place = row[7]
            self.description = row[8]
            self.series = row[9]
            self.organizers = row[10]
            self.sponsors = row[11]
            self.game_mode = row[12]
            self.format_style = row[13]
            self.team = row[14]
            self.runners_up = row[15]
            self.name = row[16]
        if in_db:
            self._verify(loader)
        else:
            self.api_tournament.load_advanced(loader)
            save_tournament(self.api_tournament)
            self._load(loader)
        self._load_first_place_results(loader)

    def _load_first_place_results(self, loader):
        self.first_place_tournaments = []

        if self.api_tournament.first_place_url:
            sql = PLAYER_RESULTS_COUNT_SQL.format(self.api_tournament.first_place_url,
                                                  self.api_tournament.url)
            for count, in execute_sql(sql):
                if count < 1:
                    save_player(self.api_tournament.first_place_url, loader)
            load_sql = PLAYER_RESULTS_SQL.format(*((self.api_tournament.first_place_url,) + TIERS + GAMES))
            for row in execute_sql(load_sql):
                result = {
                    'name': row[0],
                    'game': row[1],
                    'tier': row[2],
                    'place': row[3],
                    'date': row[4],
                    'winner': row[5],
                    'team': row[6]
                }
                self.first_place_tournaments.append(result)

    def _verify(self, loader):
        first_place_check = self.first_place == self.api_tournament.first_place or (self.first_place and self.team)
        if not all((
                self.name == self.api_tournament.name,
                self.tier == self.api_tournament.tier,
                self.start == self.api_tournament.start,
                self.end == self.api_tournament.end,
                self.prize == self.api_tournament.prize,
                self.participant_count == self.api_tournament.participant_count,
                first_place_check,
                self.first_place_url == self.api_tournament.first_place_url,
        )):
            if DEBUG:
                print_inequality(self, self.api_tournament, ("name",
                                                             "tier",
                                                             "start",
                                                             "end",
                                                             "prize",
                                                             "participant_count",
                                                             "first_place",
                                                             "first_place_url",
                ))
            self.api_tournament.load_advanced(loader)
            update_tournament(self.api_tournament)
            self._load(loader)
def print_inequality(t1, t2, attributes):
    for attribute in attributes:
        a = getattr(t1, attribute)
        b = getattr(t2, attribute)
        if a != b:
            print("{}: {} vs {}".format(attribute, a, b))
    
def arguments():
    parser = ArgumentParser()
    parser.add_argument("--date", help="Custom date to start processing in YYYYmmdd")
    return parser.parse_args()

def run():
    args = arguments()
    now = datetime.strptime(args.date, "%Y%m%d") if args.date else datetime.now()
    last_week, this_week = tournament_timeboxes(now)
    loader = TournamentLoader()
    loader.starting(this_week)
    loader.ongoing(last_week)
    loader.completed(last_week)

if __name__ == "__main__":
    run()
