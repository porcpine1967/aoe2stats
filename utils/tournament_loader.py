#!/usr/bin/env python3
""" Fetches data from db and/or liquipedia"""
from argparse import ArgumentParser

from datetime import datetime

import psycopg2

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers.tournament_manager import TournamentManager

from utils.tools import execute_sql, tournament_timeboxes

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
WHERE url = '{}'
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
    
def save_api_tournament(api_tournament, loader):
    """ Persists api_tournament to db."""
    api_tournament.load_advanced(loader)
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

def update_tournament(api_tournament, loader):
    api_tournament.load_advanced(loader)
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
    )
    sql = UPDATE_SQL.format(api_tournament.url)
    execute_transaction(sql, row)
    
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
            save_api_tournament(self.api_tournament, loader)
            self._load(loader)

    def _verify(self, loader):
        if not all((
                self.name == self.api_tournament.name,
                self.tier == self.api_tournament.tier,
                self.start == self.api_tournament.start,
                self.end == self.api_tournament.end,
                self.prize == self.api_tournament.prize,
                self.participant_count == self.api_tournament.participant_count,
                self.first_place == self.api_tournament.first_place,
                self.first_place_url == self.api_tournament.first_place_url,
                self.second_place == self.api_tournament.second_place,
        )):

            update_tournament(self.api_tournament, loader)
            self._load(loader)

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