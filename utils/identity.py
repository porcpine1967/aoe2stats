#!/usr/bin/env python3
""" Manage identity consolidation."""

from collections import defaultdict
from datetime import datetime, timedelta
import json
import os

import requests
import yaml

from utils.tools import PLAYERS_YAML, player_yaml, save_yaml, cache_file

SE_PLAYERS = "https://raw.githubusercontent.com/SiegeEngineers/aoc-reference-data/master/data/players.yaml"
AOE_ELO_PLAYERS = "https://aoe-elo.com/api/?request=players"

AOE_ELO_JSON = "tmp/aoe_elo_players.json"

def local_player_yaml():
    return player_yaml()

def aoe_elo_json():
    with open(cache_file(AOE_ELO_JSON, AOE_ELO_PLAYERS)) as f:
        return json.load(f)

def remote_player_yaml(local=True):
    local_file = "tmp/players.yaml"
    with open(cache_file(local_file, SE_PLAYERS)) as f:
        return yaml.safe_load(f)

def consolidate_player(player, remote_player):
    for key, attr in remote_player.items():
        if key in player:
            if key == 'liquipedia':
                continue
        if isinstance(attr, list):
            attr = sorted(list(set(player[key] + attr)))
        player[key] = attr

def player_names(player):
    """Returns all the names the player could be known by."""
    names = set()
    names.add(player['name'])
    names.add(player.get('canonical_name'))
    names.add(player.get('liquipedia'))
    for alias in player.get('aka', []):
        names.add(alias)
    return names
    
def consolidate_yamls():
    players = local_player_yaml()
    no_id_players = [player for player in players if 'id' not in player]
    for remote_player in remote_player_yaml():
        for player in players:
            if not 'id' in player:
                continue
            if remote_player["id"] == player["id"]:
                consolidate_player(player, remote_player)
                break
        else:
            names = player_names(remote_player)
            for player in no_id_players:
                if player['canonical_name'] in names:
                    consolidate_player(player, remote_player)
                    break
            else:
                remote_player['canonical_name'] = remote_player['name']
                players.append(remote_player)
    save_yaml(players)

def update_aoe_elo():
    players = local_player_yaml()

    elo_players = aoe_elo_json()
    for player in players:
        if not 'aoeelo' in player:
            continue
        found = False
        for elo_player in elo_players:
            if elo_player['id'] == player['aoeelo']:
                if not elo_player['name'] in player_names(player):
                    aka = player.get('aka', [])
                    aka.append(elo_player['name'])
                    player['aka'] = sorted(list(set(aka)))
                break
    save_yaml(players)
def run():
    update_aoe_elo()

if __name__ == "__main__":
    run()
