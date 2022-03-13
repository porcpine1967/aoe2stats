#!/usr/bin/env python3
""" Classes that predict the outcome of tournament brackets."""
from collections import defaultdict
from configparser import ConfigParser
from datetime import datetime, time, timedelta
import logging
import re

from liquiaoe.loaders import VcrLoader as Loader
from liquiaoe.managers import Tournament

import utils.robo_atp
from utils.identity import player_names, player_yaml
from utils.tools import execute_sql, flatten, setup_logging

LOGGER = setup_logging()

SQL_CACHE_FILE = 'tmp/sqlcache'
STARTING_ROUND = 0
POINT_SYSTEMS = {
    'uniform': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1,],
    'little': [1, 2, 3, 4, 6, 10, 16, 24,],
    'big': [1, 2, 4, 8, 16, 32, 64, 128,],
    }

class Seeder:
    def __init__(self, tournament):
        self.tournament = tournament
        self.lookup = defaultdict(int)
    def load_others(self, others):
        pass

    def score_bracket(self, point_system):
        rounds = self.tournament.rounds[STARTING_ROUND:]
        score = 0
        predictions = self.bracket_predictions()
        for idx, round_ in enumerate(rounds):
            winners = {x['winner_url'] for x in round_ if x['winner_url']}
            if idx == 0:
                missing = [unranked for unranked in winners if not self.lookup[unranked]]
                if missing:
                    LOGGER.warning("Missing {} {} {}".format(self.__class__.__name__, self.tournament.url, ", ".join(missing)))
            correct = len(winners.intersection(predictions[idx]))
            winners = sorted([x[14:] for x in winners if x])
            guesses = sorted([x[14:] for x in predictions[idx]])
            LOGGER.debug("WINNERS: {} | PREDICTIONS: {}".format(", ".join(winners), ", ".join(guesses)))
            score += correct * point_system[idx]
        return score

    def bracket_predictions(self):
        """ Returns an array of sets with predicted winners of round"""
        current_round = self.participants
        predictions = []
        while len(current_round) > 1:
            prediction = self.round_prediction(current_round)
            predictions.append({x for x in prediction if x})
            current_round = prediction
        return predictions

    def round_prediction(self, round_):
        """ Returns an ordered list of predicted winners of round"""
        predictions = []
        for idx in range(0, len(round_), 2):
            player_1 = round_[idx]
            player_2 = round_[idx + 1]
            if self.lookup[player_1] > self.lookup[player_2]:
                predictions.append(player_1)
            else:
                predictions.append(player_2)
        return predictions

    @property
    def participants(self):
        return round_participants(self.tournament, STARTING_ROUND)

class Winner(Seeder):
    def __init__(self, tournament):
        self.tournament = tournament
        self.lookup = defaultdict(lambda: 1)

    def bracket_predictions(self):
        predictions = []
        rounds = self.tournament.rounds[STARTING_ROUND:]
        for idx, round_ in enumerate(rounds):
            winners = {x['winner_url'] for x in round_ if x['winner_url']}
            predictions.append(winners)
        return predictions

class HolySeeder(Seeder):
    def load_others(self, others):
        for participant in self.participants:
            self.lookup[participant] = others[JonSlowSeeder].lookup[participant]/2
            self.lookup[participant] += others[AoeEloSeeder].lookup[participant]*4
            self.lookup[participant] += others[RoboAtpSeeder].lookup[participant]

class AoeEloSeeder(Seeder):
    SQL = """
SELECT s.player_url, s.score, s.evaluation_date
FROM scores as s,
(SELECT player_url, max(evaluation_date) as eval_date FROM scores
WHERE evaluation_date <= '{}'
AND scorer = '{}'
GROUP BY player_url) as best_date
WHERE best_date.player_url = s.player_url
AND best_date.eval_date = s.evaluation_date
AND scorer = '{}'
ORDER BY s.score
"""
    def __init__(self, tournament):
        self.tournament = tournament
        self.lookup = defaultdict(int)
        cutoff = tournament.start
        for player_url, score, _ in execute_sql(self.SQL.format(cutoff, self.scorer, self.scorer)):
            if player_url:
                self.lookup[player_url] = score
        self.unranked = None
        self.others = {}

    @property
    def scorer(self):
        return 'aoe-elo'

class RoboAtpSeeder(Seeder):
    def __init__(self, tournament):
        self.tournament = tournament
        self.lookup = defaultdict(int)
        for name, rating in utils.robo_atp.player_ratings(tournament.start).items():
            try:
                player = LIQUIPEDIA_LOOKUP[name]
                self.lookup["/ageofempires/{}".format(player.get('liquipedia'))] = rating
            except KeyError:
                LOGGER.debug("{} not in liquipedia".format(name))
        self.unranked = None
        self.others = {}

class DBSeeder(Seeder):
    def __init__(self, tournament):
        self.load_cache()
        self.tournament = tournament
        self.lookup = defaultdict(int)
        cutoff = datetime.combine(tournament.start, time()).timestamp()
        players = player_yaml()
        for participant in self.participants:
            if not participant:
                continue
            liquipedia_name = participant.split('/')[-1]

            for player in players:
                if liquipedia_name == player.get('liquipedia'):
                    try:
                        pids = [x for x in player['platforms']['rl'] if 'n' not in x]
                        pid_str = ",".join(pids)
                        sql = self.SQL.format(pids=pid_str, started=cutoff)
                        self.lookup[participant] = self.rating_from_sql(sql.strip(), participant)
                    except KeyError:
                        break
        self.save_cache()
    def save_cache(self):
        with open(SQL_CACHE_FILE, 'w') as f:
            self.config.write(f)
    @property
    def cache_section(self):
        return self.__class__.__name__
    def load_cache(self):
        self.config = ConfigParser(delimiters=("@",))
        self.config.read(SQL_CACHE_FILE)
        if not self.cache_section  in self.config:
            self.config[self.cache_section] = {}
        self.cache = self.config[self.cache_section]

def round_participants(tournament, round_index):
    round_ = tournament.rounds[round_index]
    return flatten([[x['winner_url'], x['loser_url'],] for x in round_])

class RankedLadderSeeder(DBSeeder):
    SQL = """
SELECT m.rating, c.mrating
FROM matches m,
(SELECT max(started) as mstarted, max(rating) as mrating, player_id
FROM matches
WHERE
player_id IN ({pids})
AND game_type = 0
AND team_size = 1
AND rating IS NOT NULL
AND started < {started:0.0f}
GROUP BY player_id) as c
WHERE m.player_id = c.player_id
AND m.started = c.mstarted
""".replace("\n", " ").lower()
    def rating_from_sql(self, sql, _):
        if sql in self.cache:
            return int(self.cache[sql].split(',')[0])
        LOGGER.debug(sql)
        hold_current = hold_max = 0
        for current_rating, max_rating in execute_sql(sql):
            if max_rating > hold_max:
                hold_max = max_rating
            if current_rating > hold_current:
                hold_current = current_rating
            self.cache[sql] = ",".join((str(hold_current), str(hold_max),))
        return hold_current

class JonSlowSeeder(RankedLadderSeeder):
    def rating_from_sql(self, sql, _):
        if sql in self.cache:
            return sum([int(x) for x in self.cache[sql].split(',')])
        LOGGER.debug(sql)
        hold_current = hold_max = 0
        for current_rating, max_rating in execute_sql(sql):
            if max_rating > hold_max:
                hold_max = max_rating
            if current_rating > hold_current:
                hold_current = current_rating
            self.cache[sql] = ",".join((str(hold_current), str(hold_max),))
        return hold_current + hold_max
    @property
    def cache_section(self):
        return 'RankedLadderSeeder'

class OgnSeeder(Seeder):
    def load_others(self, others):
        for participant in self.participants:
            aoe_elo_score = others[AoeEloSeeder].lookup[participant]
            jon_slow_score = others[JonSlowSeeder].lookup[participant]
            self.lookup[participant] = aoe_elo_score*2 + jon_slow_score

def run():
    loader = Loader()
    s_tournament_urls = (
        '/ageofempires/King_of_the_Desert/4',
        '/ageofempires/Holy_Cup',
        '/ageofempires/The_Open_Classic',
        '/ageofempires/Hidden_Cup/4',
        '/ageofempires/Wandering_Warriors_Cup',
        '/ageofempires/Red_Bull_Wololo/3',
        '/ageofempires/Red_Bull_Wololo/4',
        '/ageofempires/Red_Bull_Wololo/5',
        )
    a_tournament_urls = (
        '/ageofempires/German_Championship/2021',
        '/ageofempires/Master_of_Socotra/1',
        '/ageofempires/European_Rumble',
        '/ageofempires/Arabia_Invitational/2',
        '/ageofempires/Yolo_Cup',
        '/ageofempires/Ellie%27s_Charity_Invitational/2',
        '/ageofempires/Masters_of_Arena/6',
        '/ageofempires/All-In_Cup',
        '/ageofempires/Visible_Cup/4',
        '/ageofempires/Exiled_Heroes',
        '/ageofempires/History_Hit_Open',
        '/ageofempires/Rusaoc_Cup/77',
    )
    seeders = (RankedLadderSeeder, JonSlowSeeder, AoeEloSeeder, RoboAtpSeeder, OgnSeeder, HolySeeder, Winner)
    seeder_totals = {}
    for seeder in seeders:
        seeder_totals[seeder] = defaultdict(int)
    for tier, tournament_urls in (('S-Tier', s_tournament_urls,), ('A-Tier', a_tournament_urls,),):
        for url in tournament_urls:
            tournament_name = re.sub(r'[/_]', ' ', url[14:])
            tournament = Tournament(url)
            tournament.load_advanced(loader)
            others = {}
            for seed_class in seeders:
                seeder = seed_class(tournament)
                others[seed_class] = seeder
                seeder.load_others(others)
                for name, system in POINT_SYSTEMS.items():
                    totals = seeder_totals[seed_class]
                    score = seeder.score_bracket(system)
                    key = "{}:{}".format(tier, name)
                    all_key = "All:{}".format(name)
                    totals[key] += score
                    totals[all_key] += score

    print('*'*28)
    print("TOTALS")
    print('*'*28)
    headers = ['S-Tier:uniform', 'A-Tier:uniform', 'All:uniform','S-Tier:little', 'A-Tier:little', 'All:little', 'S-Tier:big', 'A-Tier:big', 'All:big',]
    template = "{:22} {:3} {:3} {:3} {:3} {:3} {:3} {:3} {:3} {:3}"
    print(template.format('System', *headers))
    for seeder, totals in seeder_totals.items():
        print(template.format(seeder.__name__, *[totals[key] for key in headers]))
if __name__ == '__main__':
    LIQUIPEDIA_LOOKUP = {}
    for player in player_yaml():
        for alias in player_names(player):
            LIQUIPEDIA_LOOKUP[alias] = player
    MISSING = set()
    run()
    for m in MISSING:
        print(m)
