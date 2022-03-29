#!/usr/bin/env python3
""" Fetches data from db and/or liquipedia"""
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
import logging
import os
import re

import psycopg2

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import PlayerManager, TournamentManager

from utils.tools import execute_sql, execute_transaction, tournament_timeboxes
from utils.tools import setup_logging, LOGGER_NAME
from utils.identity import player_yaml, players_by_name, save_yaml, canonical_identifiers

PLAYERS = player_yaml()

LOGGER = logging.getLogger(LOGGER_NAME)

GAMES = ("Age of Empires II", "Age of Empires IV",)
TIERS = ("S-Tier", "A-Tier", "B-Tier",)

UPSET_WORTHY_PATH = 'cache/upset_worthy'

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

PLAYER_RESULTS_EXIST_SQL = """
SELECT id FROM player_results
WHERE (player_name = '{}' OR player_name = '{}' OR player_url = '{}')
AND tournament_url = '{}'
LIMIT 1
"""

PLAYER_EXISTS_SQL = """
SELECT id FROM player_results
WHERE player_name = '{}'
OR player_url = '{}'
LIMIT 1
"""

TOURNAMENT_EXISTS_SQL = """
SELECT id FROM tournaments
WHERE url = '{}'
LIMIT 1
"""

SAVE_PLAYER_RESULTS_SQL = """
INSERT INTO player_results
(player_url, player_place, player_prize, player_name, tournament_url)
VALUES (%s, %s, %s, %s, %s)
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
tournaments.team,
results.player_prize
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

    def ongoing(self, timestamp):
        """ Fetch information on all ongoing tournaments."""
        return self._tournament_dict(self.tournament_manager.ongoing(timestamp).items())

    def completed(self, timebox):
        """ Fetch information on all completed tournaments."""
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
            if api_tournament.cancelled:
                continue
            tournament = Tournament(api_tournament, self.loader)
            db_tournaments.append(tournament)
        return db_tournaments

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


def save_player(tournament_url, player_name, player_url, placement, prize, loader):
    if player_result_present(player_name, player_url, tournament_url):
        return
    row = (player_url,
           placement,
           prize,
           player_name,
           tournament_url,)
    execute_transaction(SAVE_PLAYER_RESULTS_SQL, row)
    for _ in execute_sql(PLAYER_EXISTS_SQL.format(player_name, player_url)):
        break
    else:
        if player_url:
            player_manager = PlayerManager(loader)
            for api_tournament in player_manager.tournaments(player_url):
                row = (player_url,
                       api_tournament.loader_place,
                       api_tournament.loader_prize,
                       player_name,
                       api_tournament.url,)
                execute_transaction(SAVE_PLAYER_RESULTS_SQL, row)
                # Only save basic attributes; no updating
                for _ in execute_sql(TOURNAMENT_EXISTS_SQL.format(api_tournament.url)):
                    break
                else:
                    save_tournament(api_tournament)

def placement_results(url):
    results = []
    sql = PLAYER_RESULTS_SQL.format(*((url,) + TIERS + GAMES))
    for row in execute_sql(sql):
        result = {
            'name': row[0],
            'game': row[1],
            'tier': row[2],
            'place': row[3],
            'date': row[4],
            'winner': row[5],
            'team': row[6],
            'prize': row[7],
        }
        results.append(result)
    return results

def player_result_present(player_name, player_url, tournament_url):
    sql = PLAYER_RESULTS_EXIST_SQL.format(player_name,
                                          player_name.capitalize(),
                                          player_url,
                                          tournament_url)
    for _ in execute_sql(sql):
        return True
    return False

def ratings():
    player_lookup = players_by_name()
    def dd():
        return defaultdict(lambda: 100000)
    _ratings = defaultdict(dd)
    with open("{}/Documents/podcasts/aoe2/current/ratings.txt".format(os.getenv('HOME'))) as f:
        for l in f:
            if l.startswith('  .'):
                continue
            rank, atp, telo = re.split(r'  +', l.strip())
            try:
                _ratings[player_lookup[atp]['liquipedia']]['ATP'] = int(rank[:-1])
            except KeyError:
                pass
            try:
                _ratings[player_lookup[telo]['liquipedia']]['TELO'] = int(rank[:-1])
            except KeyError:
                pass
    return _ratings

class Tournament:
    def __init__(self, api_tournament, loader):
        self.api_tournament = api_tournament
        self.url = api_tournament.url
        self.first_place_tournaments = []
        self.loader = loader
        self._load(loader)

    @property
    def check_for_upsets(self):
        if self.start > datetime.now().date() or self.team:
            return False
        last_letter = ''
        with open(UPSET_WORTHY_PATH) as f:
            for l in f:
                if self.url in l:
                    last_letter = l.strip()[-1]
        if last_letter:
            return last_letter == 'T'
        self.api_tournament.load_advanced(self.loader)
        if not self.api_tournament.participants:
            return False
        r = ratings()
        rated_players = len({x for x in self.api_tournament.participants if x[0] in r})
        should_check = rated_players > 1
        with open(UPSET_WORTHY_PATH, 'a') as f:
            f.write("\n{}: {}".format(self.url, should_check and 'T'))
        return should_check

    @property
    def upsets(self):
        _upsets = []
        if not self.check_for_upsets:
            return upsets
        _ratings = ratings()
        self.api_tournament.load_advanced(self.loader)
        for match in self.api_tournament.matches:
            winner = match.winner
            loser = match.loser
            if _ratings[winner]['ATP'] > _ratings[loser]['ATP'] and _ratings[winner]['TELO'] > _ratings[loser]['TELO']:
                _upsets.append(match)
        return _upsets
        
    def _load(self, loader):
        sql = FROM_SQL.format(self.url)
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
        self._load_placement_results(loader)

    def _verify_participant_placements(self, loader):
        if self.team or not self.first_place:
            return
        name, url = canonical_identifiers(self.first_place, self.first_place_url, PLAYERS)
        if not player_result_present(name, url, self.url):
            self.api_tournament.load_advanced(loader)
            for player_name, player_url, placement, prize in self.api_tournament.participants:
                if self.api_tournament.game == 'Age of Empires II' and placement:
                    name, url = canonical_identifiers(player_name, player_url, PLAYERS)
                    save_player(self.url, name, url, placement, prize, loader)
                if player_url and placement:
                    save_player(self.url, player_name, player_url, placement, prize, loader)

    def _load_placement_results(self, loader):
        self._verify_participant_placements(loader)

        if self.first_place_url:
            self.first_place_tournaments = placement_results(self.first_place_url)


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
            self.log_inequality()
            self.api_tournament.load_advanced(loader)
            update_tournament(self.api_tournament)
            self._load(loader)

    def log_inequality(self):
        inequalities = []
        for attribute in ("name", "tier", "start", "end", "prize", "participant_count", "first_place", "first_place_url", ):
            a = getattr(self, attribute)
            b = getattr(self.api_tournament, attribute)
            if a != b:
                inequalities.append("{}: {} vs {}".format(attribute, a, b))
        LOGGER.debug("Updating {} because of inequalities: {}".format(self.name, ", ".join(inequalities)))

def arguments():
    parser = ArgumentParser()
    parser.add_argument("--date", help="Custom date to start processing in YYYYmmdd")
    parser.add_argument('--debug', action='store_true', help="Set logger to debug")
    args = parser.parse_args()
    if args.debug:
        setup_logging(logging.DEBUG)
    else:
        setup_logging()

    return args

def run():
    args = arguments()
    now = datetime.strptime(args.date, "%Y%m%d") if args.date else datetime.now()
    last_week, this_week = tournament_timeboxes(now)
    loader = TournamentLoader()
    loader.starting(this_week)
    loader.ongoing(now.date())
    loader.completed(last_week)
    save_yaml(PLAYERS)

if __name__ == "__main__":
    run()
