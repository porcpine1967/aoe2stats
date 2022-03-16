#!/usr/bin/env python3
""" Manage identity consolidation."""

from collections import defaultdict
from datetime import datetime, timedelta
import json
import os

import requests
import yaml

from utils.tools import cache_file

PLAYERS_YAML = "data/players.yaml"


SE_PLAYERS = ("cache/players.yaml",
              "https://raw.githubusercontent.com/SiegeEngineers/aoc-reference-data/master/data/players.yaml",)
AOE_ELO_PLAYERS = ("cache/aoe_elo_players.json",
                   "https://aoe-elo.com/api/?request=players",)

def canonical_identifiers(player_name, player_url, players):
    """ Returns canonical_name, liquipedia_url if in players.yaml
        updates (but not saves) liquipedia if player_url and no liquipedia entry in players. """
    identifiers = {player_name}
    if player_url:
        liquipedia_name = player_url.split('/')[-1]
        identifiers.add(liquipedia_name)
    for player in players:
        if player.get('ignore'):
            continue
        if identifiers.intersection(player_names(player)):
            if 'liquipedia' in player:
                return (player['canonical_name'], '/ageofempires/{}'.format(player['liquipedia']),)
            else:
                if player_url:
                    player['liquipedia'] = liquipedia_name
                return (player['canonical_name'], player_url,)
    return (player_name, player_url)

def player_yaml():
    with open(PLAYERS_YAML) as f:
        return yaml.safe_load(f)

def save_yaml(players):
    with open(PLAYERS_YAML, "w") as f:
        yaml.dump(players, f)

def aoe_elo_players():
    cache_file(*AOE_ELO_PLAYERS)
    with open(AOE_ELO_PLAYERS[0]) as f:
        return json.load(f)

def se_players():
    cache_file(*SE_PLAYERS)
    with open(SE_PLAYERS[0]) as f:
        return yaml.safe_load(f)

def consolidate_player(player, se_player):
    for key, attr in se_player.items():
        if key in player:
            if key == 'liquipedia':
                continue
        if isinstance(attr, list):
            attr = sorted(list(set(player.get(key, list()) + attr)))
        player[key] = attr

def player_names(player):
    """Returns all the names the player could be known by."""
    names = set()
    names.add(player['name'])
    names.add(player.get('canonical_name'))
    names.add(player.get('liquipedia'))
    for alias in player.get('aka', []):
        names.add(alias)
    return {name for name in names if name}

def consolidate_yamls():
    local_players = player_yaml()
    no_id_players = [local_player for local_player in local_players if 'id' not in local_player]
    for se_player in se_players():
        for local_player in local_players:
            if se_player["id"] == local_player.get("id"):
                consolidate_player(local_player, se_player)
                break
        else:
            names = player_names(se_player)
            for local_player in no_id_players:
                if local_player['canonical_name'] in names:
                    consolidate_player(local_player, se_player)
                    break
            else:
                se_player['canonical_name'] = se_player['name']
                local_players.append(se_player)
    save_yaml(local_players)

def update_aoe_elo():
    local_players = player_yaml()

    elo_players = aoe_elo_players()
    for local_player in local_players:
        if not 'aoeelo' in local_player:
            continue
        found = False
        for elo_player in elo_players:
            if elo_player['id'] == local_player['aoeelo']:
                if not elo_player['name'] in player_names(local_player):
                    aka = local_player.get('aka', [])
                    aka.append(elo_player['name'])
                    local_player['aka'] = sorted(list(set(aka)))
                break
    save_yaml(local_players)

def verify_name_uniqueness():
    checked_names = set()
    bads = set()
    for player in player_yaml():
        if player.get('ignore'):
            continue
        names = player_names(player)
        bads.update(names.intersection(checked_names))
        checked_names.update(names)
    print(bads)
def run():
    consolidate_yamls()

if __name__ == "__main__":
    run()
