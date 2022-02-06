#!/usr/bin/env python3
""" Update data/players.yaml with new information."""

from collections import defaultdict
from datetime import datetime, timedelta
import json
import os

import requests
import yaml

SE_PLAYERS = "https://raw.githubusercontent.com/SiegeEngineers/aoc-reference-data/master/data/players.yaml"
AOE_ELO_PLAYERS = "https://aoe-elo.com/api/?request=players"

PLAYERS_YAML = "data/players.yaml"
AOE_ELO_JSON = "tmp/aoe_elo_players.json"
def save_yaml(players):
    with open(PLAYERS_YAML, "w") as f:
        yaml.dump(players, f)

def local_player_yaml():
    with open(PLAYERS_YAML) as f:
        return yaml.safe_load(f)

def aoe_elo_json():
    players = []
    with open(AOE_ELO_JSON) as f:
        for player in json.load(f):
            players.append(defaultdict(str, player))
    return players
    
def remote_player_yaml(local=True):
    local_file = "tmp/players.yaml"
    if local and os.path.exists(local_file):
        mtime = datetime.fromtimestamp(os.stat(local_file).st_mtime)
        if mtime < datetime.now() - timedelta(days=1):
            return remote_player_yaml(False)
        with open(local_file) as f:
            return yaml.safe_load(f)
    else:
        response = requests.get(SE_PLAYERS)
        data = response.text
        with open(local_file, "w") as f:
            for l in data:
                f.write(l)
    return remote_player_yaml()

def consolidate_yamls():
    players = local_player_yaml()
    for remote_player in remote_player_yaml():
        for player in players:
            if remote_player["id"] == player["id"]:
                for key, attr in remote_player.items():
                    if key in player:
                        if key == 'liquipedia':
                            continue
                        if isinstance(attr, list):
                            attr = sorted(list(set(player[key] + attr)))
                    player[key] = attr
                break
        else:
            players.append(remote_player)
    with open(PLAYERS_YAML, "w") as f:
        yaml.dump(players, f)

def update_aoe_elo():
    players = local_player_yaml()
    
    with open("tmp/aoe_elo_players.json") as f:
        elo_players = json.load(f)
    found_ctr = Counter()
    for player in players:
        if not 'aoeelo' in player:
            continue
        found = False
        for elo_player in elo_players:
            if elo_player['id'] == player['aoeelo']:
                if elo_player['name'] == player['name']:
                    found = True
                    if 'akxxxxa' in player:
                        for alias in player['aka']:
                            if player['name'] == alias:
                                found = True
        if not found:
            print(elo_player['name'], '|',  player['name'])
def standard_name(name):
    return name.replace('_', ' ').lower().strip()
    
def test_robo_atp():
    players = local_player_yaml()
    elo_players = aoe_elo_json()
    robo_players = []
    with open("tmp/roboatp.csv") as f:
        for l in f:
            robo_players.append(l.strip())
    missing = []
    for robo_player in robo_players:
        if robo_player in ("_MyST_eTaS", "Eli", "Cyborg", "Twistzz",):
            continue
        name = standard_name(robo_player)
        found = False
        for player in players:
            if name == standard_name(player['name']):
                found = True
                break
            if name == standard_name(player['liquipedia']):
                found = True
                break
            for alias in player['aka']:
                if name == standard_name(alias):
                    found = True
                    break
        if not found:
            for elo_player in elo_players:
                if name == standard_name(elo_player['name']):
                    found = True
                    break
        if not found:
            missing.append(robo_player)
    for m in missing:
        print(m)
def run():
    consolidate_yamls()

if __name__ == "__main__":
    run()
