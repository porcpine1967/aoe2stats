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
import requests

from liquiaoe.loaders import VcrLoader, RequestsException, THROTTLE
from liquiaoe.managers import Tournament

from utils.tools import execute_sql, execute_bulk_insert, player_yaml, save_yaml
from utils.tools import LOGGER_NAME, setup_logging

SAVE_SCORES_SQL = """INSERT INTO scores
(evaluation_date, player_url, score, scorer)
VALUES %s
ON CONFLICT DO NOTHING
"""

PLAYERS_CACHE = "tmp/aoe_elo_players.json"

UPDATED_ATTRIBUTE = 'aoeelo_updated'
SCORER = 'aoe-elo'
LOGGER = logging.getLogger(LOGGER_NAME)

class AoeEloLoader:
    def __init__(self):
        self.last_call = 0
        self._player_list_url = "https://aoe-elo.com/api?request=players"
        self._headers = {"User-Agent": "aoe2stats/0.1 (feroc.felix@gmail.com)","Accept-Encoding": "gzip"}
        self._base_url = "https://liquipedia.net/ageofempires/api.php?redirects=true&action=parse&format=json&page={}"
        self._player_dict = None

    def player_page(self, player_id):
        """ Returns the string version of the player page"""
        if player_id in self.players:
            return self.response_text(self.players[player_id])
        return ''

    @property
    def players(self):
        if not self._player_dict:
            load_file = None
            self._player_dict = {}
            if os.path.exists(PLAYERS_CACHE):
                mtime = datetime.fromtimestamp(os.stat(PLAYERS_CACHE).st_mtime)
                if mtime > datetime.now() - timedelta(days=1):
                    load_file = PLAYERS_CACHE
            if load_file:
                with open(load_file) as f:
                    data = json.load(f)
            else:
                data = json.loads(self.response_text(self._player_list_url))
            for player in data:
                self._player_dict[player['id']] = player['url']
        return self._player_dict

    def response_text(self, url):
        LOGGER.warning("Calling aoe-elo url {}".format(url))
        if self.last_call + THROTTLE > time.time():
            time.sleep(self.last_call + THROTTLE - time.time())
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
        if player['id'] == yaml_player['id']:
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

def players_to_update(tournament_url):
    players = []
    lookup = player_lookup()
    tournament = Tournament(tournament_url)
    tournament.load_advanced(VcrLoader())
    for name, url, _ in tournament.participants:
        if not url:
            continue
        url_name = url.split('/')[-1]
        player = lookup[url_name]
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

def run():
    args = arguments()
    loader = AoeEloLoader()
    players = players_to_update(args.tournament_url)
    for player in players:
        update_player(loader, player)

if __name__ == '__main__':
    run()
