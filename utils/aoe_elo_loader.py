#!/usr/bin/env python3
""" Ensures aoe-elo data is available for date."""
from argparse import ArgumentParser
from datetime import date, datetime
import json
import re

from bs4 import BeautifulSoup

from liquiaoe.loaders import VcrLoader
from liquiaoe.managers import Tournament

from utils.tools import execute_sql, player_yaml

def player_lookup():
    lookup = {}
    for player in player_yaml():
        if 'liquipedia' in player:
            lookup[player['liquipedia']] = player
    return lookup

def date_score_pairs(script_text):
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
                    continue
                score_html = scores[idx]
                score = re.split(r'</?span[^>]*>', score_html)[-2]
                pairs.append((score, date,))
    return pairs
    
def update_player(aoe_elo_id):
    with open('tmp/v.html') as f:
        soup = BeautifulSoup(f, "html.parser")
    for script in soup.find_all('script'):
        if 'page.eloDevChart.chartData' in script.text:
            pairs = date_score_pairs(script.text)
            break
    else:
        return
    print(len(pairs))
                    
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
        try:
            if player['aoe_elo_updated'] > tournament.start:
                continue
        except KeyError:
            pass
        players.append(player)
    print(len(players))

def arguments():
    parser = ArgumentParser()
    parser.add_argument('-tournament_url', type=str, help="Liquipedia tournament url",
                        default="/ageofempires/Wandering_Warriors_Cup")
    return parser.parse_args()

def run():
    t = Tournament(arguments().tournament_url)
    t.load_advanced(VcrLoader())
    for p in t.participants:
        print(p)

if __name__ == '__main__':
    update_player('foo')
