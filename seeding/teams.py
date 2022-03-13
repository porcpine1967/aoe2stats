#!/usr/bin/env python3
""" Classes that predict the outcome of team tournament brackets."""
from collections import defaultdict
from configparser import ConfigParser
from datetime import datetime, time, timedelta
import logging
import re

from liquiaoe.loaders import VcrLoader as Loader
from liquiaoe.managers import Tournament, TournamentManager

from utils.identity import player_names, player_yaml
from utils.tools import execute_sql, flatten, setup_logging

LOGGER = setup_logging()

REPLACEMENTS = {
    '/ageofempires/Mr_Yo': '/ageofempires/Yo',
    '/ageofempires/Mr._Yo': '/ageofempires/Yo',
    '/ageofempires/SpringTV': '/ageofempires/Spring',
    '/ageofempires/Bruh': '/ageofempires/BruH',
    '/ageofempires/Gkt_cloud': '/ageofempires/Cloud_(Taiwanese_player)',
    }

SQL_CACHE_FILE = 'tmp/sqlcache'
STARTING_ROUND = 0
POINT_SYSTEMS = {
    'uniform': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1,],
    'little': [1, 2, 3, 4, 6, 10, 16, 24,],
    'big': [1, 2, 4, 8, 16, 32, 64, 128,],
    }

def tournaments():
    tiers = defaultdict(list)
    m = TournamentManager(Loader(), '/ageofempires/Age_of_Empires_II/Tournaments/Post_2020')
    for tournament in m.all():
        if tournament.start.year < 2021:
            continue
        if not tournament.team:
            continue
        tiers[tournament.tier].append(tournament)
    for tier in ("S-Tier", "A-Tier",):
        print(tier)
        print('[LIST]')
        for tournament in tiers[tier]:
            print("[*] [URL='https://liquipedia.net{}']{}[/URL]".format(tournament.url, tournament.name))
        print('[/LIST]')

class Seeder:
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
                try:
                    for name in player_names(LIQUIPEDIA_LOOKUP[player_url[14:]]):
                        self.lookup["/ageofempires/{}".format(name)] = score
                except KeyError:
                    pass
        self.load_teams()

    def load_teams(self):
        for name, info in self.tournament.teams.items():
            members = info['members']
            scores = []
            missing = []
            for player_name, url in members:
                url_from_name = "/ageofempires/{}".format(player_name)
                score = self.lookup[url] or self.lookup[url_from_name]
                if score:
                    scores.append(score)
                else:
                    missing.append(player_name)
                    MISSING.add(player_name)
            if len(scores) != len(members):
                LOGGER.debug("https://liquipedia.net{:100} {}".format(self.tournament.url, ", ".join(missing)))
            try:
                self.lookup[name] = sum(scores)/len(scores)
            except ZeroDivisionError:
                self.lookup[name] = 0

    def load_others(self, others):
        pass

    def score_bracket(self, point_system):
        rounds = self.tournament.rounds[STARTING_ROUND:]
        score = 0
        predictions = self.bracket_predictions()
        for idx, round_ in enumerate(rounds):
            winners = {x['winner'] for x in round_ if x['winner']}
            if idx == 0:
                self.unranked = [unranked for unranked in winners if not self.lookup[unranked]]
            correct = len(winners.intersection(predictions[idx]))
            LOGGER.debug("WINNERS: {} | PREDICTIONS: {} | CORRECT: {}".format(", ".join(winners), ", ".join(predictions[idx]), correct))
            score += correct * point_system[idx]
        return score

    def bracket_predictions(self):
        """ Returns an array of sets with predicted winners of round"""
        rounds = self.tournament.rounds[STARTING_ROUND:]
        predictions = []
        for idx in range(len(rounds)):
            current_round = round_participants(self.tournament, idx)
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
    @property
    def scorer(self):
        return 'foo'
    def bracket_predictions(self):
        predictions = []
        rounds = self.tournament.rounds[STARTING_ROUND:]
        for idx, round_ in enumerate(rounds):
            winners = {x['winner'] for x in round_ if x['winner']}
            predictions.append(winners)
        return predictions


class AoeEloSeeder(Seeder):
    @property
    def scorer(self):
        return 'aoe-elo'

class RoboAtpSeeder(Seeder):
    @property
    def scorer(self):
        return 'robo-atp'

class RankedRoboAtpSeeder(RoboAtpSeeder):
    def load_teams(self):
        ratings = defaultdict(int)
        rank = len(self.lookup)
        last_score = 0
        for player in sorted(self.lookup, key=lambda x: self.lookup[x], reverse=True):
            if last_score != self.lookup[player]:
                rank -= 1
                last_score = self.lookup[player]
            ratings[player] = rank
        for name, info in self.tournament.teams.items():
            members = info['members']
            scores = []
            missing = []
            for player_name, url in members:
                url_from_name = "/ageofempires/{}".format(player_name)
                score = ratings[url] or ratings[url_from_name]
                if score:
                    scores.append(score)
                else:
                    scores.append(rank)
            try:
                self.lookup[name] = sum(scores)/len(scores)
            except ZeroDivisionError:
                self.lookup[name] = 0

class DBSeeder(Seeder):
    def __init__(self, tournament):
        self.load_cache()
        self.tournament = tournament
        self.lookup = defaultdict(int)
        cutoff = datetime.combine(tournament.start, time()).timestamp()
        players = player_yaml()
        for name, info in self.tournament.teams.items():
            members = info['members']
            for player_name, participant in members:
                if participant:
                    if participant in REPLACEMENTS:
                        participant = REPLACEMENTS[participant]
                    liquipedia_name = participant.split('/')[-1]
                else:
                    participant = "/ageofempires/{}".format(player_name)
                    liquipedia_name = player_name
                try:
                    player = LIQUIPEDIA_LOOKUP[liquipedia_name]
                    pids = [x for x in player['platforms']['rl'] if 'n' not in x]
                    pid_str = ",".join(pids)
                    sql = self.SQL.format(pids=pid_str, started=cutoff)
                    score = self.rating_from_sql(sql.strip(), participant)
                    for alias in player_names(LIQUIPEDIA_LOOKUP[participant[14:]]):
                        self.lookup["/ageofempires/{}".format(alias)] = score
                except KeyError:
                    pass
        self.save_cache()
        self.load_teams()

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
    return flatten([[x['winner'], x['loser'],] for x in round_])

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

class TeamRankedLadderSeeder(RankedLadderSeeder):
    SQL = """
SELECT m.rating, c.mrating
FROM matches m,
(SELECT max(started) as mstarted, max(rating) as mrating, player_id
FROM matches
WHERE
player_id IN ({pids})
AND game_type = 0
AND team_size > 1
AND rating IS NOT NULL
AND started < {started:0.0f}
GROUP BY player_id) as c
WHERE m.player_id = c.player_id
AND m.started = c.mstarted
""".replace("\n", " ").lower()

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

class TeamJonSlowSeeder(JonSlowSeeder):
    SQL = """
SELECT m.rating, c.mrating
FROM matches m,
(SELECT max(started) as mstarted, max(rating) as mrating, player_id
FROM matches
WHERE
player_id IN ({pids})
AND game_type = 0
AND team_size > 1
AND rating IS NOT NULL
AND started < {started:0.0f}
GROUP BY player_id) as c
WHERE m.player_id = c.player_id
AND m.started = c.mstarted
""".replace("\n", " ").lower()
    @property
    def cache_section(self):
        return 'TeamRankedLadderSeeder'

class OgnSeeder(Seeder):
    def load_others(self, others):
        for participant in self.participants:
            aoe_elo_score = others[AoeEloSeeder].lookup[participant]
            jon_slow_score = others[JonSlowSeeder].lookup[participant]
            self.lookup[participant] = aoe_elo_score*2 + jon_slow_score

class TeamOgnSeeder(OgnSeeder):
    def load_others(self, others):
        for participant in self.participants:
            aoe_elo_score = others[AoeEloSeeder].lookup[participant]
            jon_slow_score = others[TeamJonSlowSeeder].lookup[participant]
            self.lookup[participant] = aoe_elo_score*2 + jon_slow_score

class HolySeeder(OgnSeeder):
    def load_others(self, others):
        for participant in self.participants:
            self.lookup[participant] = others[JonSlowSeeder].lookup[participant]/2
            self.lookup[participant] += others[AoeEloSeeder].lookup[participant]*4
            self.lookup[participant] += others[RoboAtpSeeder].lookup[participant]

def run():
    loader = Loader()
    s_tournament_urls = (
        '/ageofempires/Empire_Wars_Duo/2',
        '/ageofempires/Two_Pools_Tournament/2',
        )
    a_tournament_urls = (
        '/ageofempires/Rage_Forest/2',
        '/ageofempires/World_Desert_Championship',
        '/ageofempires/Drunken_Forest/Scotch_League',
    )
    seeders = (AoeEloSeeder, RankedLadderSeeder, JonSlowSeeder, TeamRankedLadderSeeder, TeamJonSlowSeeder, OgnSeeder, TeamOgnSeeder, RoboAtpSeeder, RankedRoboAtpSeeder, HolySeeder, Winner)
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
    headers = ['S-Tier:little', 'A-Tier:little', 'All:little', 'S-Tier:uniform', 'A-Tier:uniform', 'All:uniform', 'S-Tier:big', 'A-Tier:big', 'All:big',]
    template = "{:22} {:3} {:3} {:3} {:3} {:3} {:3} {:3} {:3} {:3}"
    print(template.format('System', *headers))
    for seeder, totals in seeder_totals.items():
        print(template.format(seeder.__name__, *[totals[key] for key in headers]))

if __name__ == '__main__':
    # LIQUIPEDIA_LOOKUP = {}
    # for player in player_yaml():
    #     for alias in player_names(player):
    #         LIQUIPEDIA_LOOKUP[alias] = player
    # MISSING = set()
    # run()
    s_tournament_urls = (
        '/ageofempires/Empire_Wars_Duo/2',
        '/ageofempires/Two_Pools_Tournament/2',
        )
    a_tournament_urls = (
        '/ageofempires/Rage_Forest/2',
        '/ageofempires/World_Desert_Championship',
        '/ageofempires/Drunken_Forest/Scotch_League',
    )
    tournaments()
