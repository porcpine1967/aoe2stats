#!/usr/bin/env python3
""" Ensures aoe-elo data is available for date."""
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
import json
import logging
import os
import re
import time

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
import requests

from liquiaoe.loaders import RequestsException, THROTTLE
from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import Tournament, class_in_node

from utils.identity import player_yaml, save_yaml
from utils.tools import execute_sql, execute_bulk_insert
from utils.tools import LOGGER_NAME, setup_logging
from utils.tools import cache_file

SAVE_SCORES_SQL = """INSERT INTO scores
(evaluation_date, player_url, score, scorer)
VALUES %s
ON CONFLICT DO NOTHING
"""

PLAYERS_URL = "https://aoe-elo.com/api?request=players"
PLAYERS_CACHE = "cache/aoe_elo_players.json"

UPDATED_ATTRIBUTE = 'aoeelo_updated'
SCORER = 'aoe-elo'
LOGGER = logging.getLogger(LOGGER_NAME)
ELO_PATTERN = re.compile(r'/player/([0-9]+)')

class AoeEloLoader:
    def __init__(self):
        self.last_call = 0
        self._player_list_url = PLAYERS_URL
        self._headers = {"User-Agent": "aoe2stats/0.1 (feroc.felix@gmail.com)","Accept-Encoding": "gzip"}
        self._player_dict = None

    def player_page(self, player_id):
        """ Returns the string version of the player page"""
        if player_id in self.players:
            return self.response_text(self.players[player_id])
        return ''

    @property
    def current_players(self):
            use_cache = cache_file(PLAYERS_CACHE, self._player_list_url)
            if not use_cache:
                self.last_call = time.time()
            with open(PLAYERS_CACHE) as f:
                return json.load(f)

    @property
    def players(self):
        if not self._player_dict:
            load_file = None
            self._player_dict = {}
            data = self.current_players
            for player in data:
                self._player_dict[player['id']] = player['url']
        return self._player_dict

    def response_text(self, url):
        LOGGER.warning("Calling aoe-elo url {}".format(url))
        if self.last_call + THROTTLE > time.time():
            LOGGER.debug(" throttle start")
            time.sleep(self.last_call + THROTTLE - time.time())
            LOGGER.debug(" throttle done")

        response = requests.get(url, headers=self._headers)
        self.last_call = time.time()
        if response.status_code == 200:
            return response.text
        else:
            raise RequestsException(response.text, response.status_code)

def player_lookup():
    lookup = {}
    for player in player_yaml():
        if 'liquipedia' in player:
            lookup[player['liquipedia']] = player
        if 'aoeelo' in player:
            lookup[player['aoeelo']] = player
    return lookup

def date_score_pairs(script_text, player_name):
    """ Gets the date-score pairs from the json on the aoe-elo player page"""
    pairs = []
    for line in script_text.split("\n"):
        if 'page.eloDevChart.chartData' in line:
            chart_data = line[line.index('{'): -1]
            data = json.loads(chart_data)
            dates = list(data['footers'].values())[0]
            scores = list(data['labels'].values())[0]
            for idx, date_string in enumerate(dates):
                try:
                    date = datetime.strptime(date_string, "%b %d, %Y").date()
                except(ValueError):
                    date = datetime.strptime(date_string, "%b %Y").date()
                except (TypeError):
                    LOGGER.debug("Date value '{}' in data for {}".format(date_string, player_name))
                    continue
                score_html = scores[idx]
                score = re.split(r'</?span[^>]*>', score_html)[-2]
                pairs.append((date, score,))
    return pairs

def set_aoeelo_updated(player):
    players = player_yaml()
    for yaml_player in players:
        if player['id'] == yaml_player.get('id'):
            yaml_player[UPDATED_ATTRIBUTE] = datetime.now().date()
            break
    save_yaml(players)

def update_player(loader, player):
    player_url = "/ageofempires/{}".format(player['liquipedia'])
    soup = BeautifulSoup(loader.player_page(player['aoeelo']), "html.parser")
    rows = []
    for script in soup.find_all('script'):
        if 'page.eloDevChart.chartData' in script.text:
            for evaluation_date, score in date_score_pairs(script.text, player['name']):
                rows.append((evaluation_date, player_url, score, SCORER,))
    if rows:
        execute_bulk_insert(SAVE_SCORES_SQL, rows)
        set_aoeelo_updated(player)
    return soup

def aoe_elo_player_per_month():
    sql_t = """
SELECT s.player_url, s.score
FROM
(SELECT player_url, score, evaluation_date
FROM scores
WHERE scorer = 'aoe-elo'
AND evaluation_date < '{}') as s
JOIN
(SELECT player_url, max(evaluation_date) as med
FROM scores
WHERE scorer = 'aoe-elo'
AND evaluation_date < '{}'
GROUP BY player_url) as d
ON s.player_url = d.player_url
WHERE s.evaluation_date = d.med
ORDER BY s.score DESC
LIMIT 20
"""
    month_players = {}
    year = 2020
    month = 1
    for ctr in range(25):
        year_plus, month_plus = divmod(ctr, 12)
        d = date(year + year_plus, month + month_plus, 1)
        key = d.strftime('%Y-%m')
        players = []
        sql = sql_t.format(d, d)
        for u, s in execute_sql(sql):
            players.append(u[14:])
        month_players[key] = players
    return month_players

def players_to_update(loader, tournament_url):
    players = []
    lookup = player_lookup()
    tournament = Tournament(tournament_url)
    tournament.load_advanced(loader)
    for name, url, _, _ in tournament.participants:
        if not url:
            continue
        url_name = url.split('/')[-1]
        try:
            player = lookup[url_name]
        except KeyError:
            LOGGER.warning("No player {}".format(url_name))
        if 'aoeelo' not in player:
            LOGGER.warning("No aoeelo for {}".format(url_name))
            continue
        try:
            if player[UPDATED_ATTRIBUTE] > tournament.start:
                continue
        except KeyError:
            pass
        players.append(player)
    return players

def arguments():
    parser = ArgumentParser()
    parser.add_argument('-tournament_url', type=str, help="Liquipedia tournament url",
                        default="/ageofempires/Wandering_Warriors_Cup")
    parser.add_argument('--debug', action='store_true', help="Set logger to debug")
    args = parser.parse_args()
    if args.debug:
        setup_logging(logging.DEBUG)
    else:
        setup_logging()
    return args

def better_players(soup, player_id, month_match):
    betters = set()
    for node in soup.find_all('div', recursive=True):
        if class_in_node('match-view', node):
            player_elo = 0
            opponent = ''
            opponent_elo = 0
            month, day, year = [x for x in node.text.split() if x][-3:]
            try:
                day = datetime.strptime(" ".join((month, day, year)), '%b %d, %Y')
            except ValueError:
                day = datetime.strptime(" ".join((day, year)), '%b %Y')
            if day.year < 2020: #month_match not in (day.strftime('%Y-%m'), (day - relativedelta(months=1)).strftime('%Y-%m')):
                continue
            for player in node.find_all('div', recursive=True):
                if class_in_node('player', player):
                    elo = 0
                    aoe_elo_id = 0
                    name = ''
                    for tag in player.descendants:
                        if tag.name == 'a':
                           aoe_elo_id = int(ELO_PATTERN.match(tag.attrs['href']).group(1))
                        elif class_in_node('elo-num', tag):
                            elo = int(tag.text)
                    if aoe_elo_id == player_id:
                        player_elo = elo
                    else:
                        opponent_id = aoe_elo_id
                        opponent_elo = elo
            if opponent_elo > player_elo:
                betters.add(opponent_id)
    return betters

def update_from_tournament():
    args = arguments()
    tournament_loader = Loader()

    loader = AoeEloLoader()
    players = players_to_update(tournament_loader, args.tournament_url)
    print("{:25}: {:2} players".format(args.tournament_url, len(players)))
    for player in players:
        update_player(loader, player)

def update_from_aoe_players():
    args = arguments()
    loader = AoeEloLoader()
    lookup = player_lookup()
    soups = {}
    for month, players in aoe_elo_player_per_month().items():
        for player in players:
            player_data = lookup[player]
            if player not in soups:
                player_url = "/ageofempires/{}".format(player)
                soups[player] = update_player(loader, player_data)
            for better_player_id in better_players(soups[player], player_data['aoeelo'], month):
                try:
                    better_player = lookup[better_player_id]
                    if UPDATED_ATTRIBUTE not in better_player:
                        LOGGER.debug("NEW PLAYER: {}".format(better_player['canonical_name']))
                        update_player(loader, better_player)
                        better_player[UPDATED_ATTRIBUTE] = datetime.now().date()
                except KeyError:
                    LOGGER.error("NO ENTRY FOR {}".format(better_player_id))
def update_from_liquipedia(urls):
    args = arguments()
    loader = AoeEloLoader()
    lookup = player_lookup()
    for player_url in urls:
        print(player_url)
        player = lookup[player_url[14:]]
        update_player(loader, player)
def run():
    loader = AoeEloLoader()
    for p in loader.players:
        print(p)
if __name__ == '__main__':
    run()
