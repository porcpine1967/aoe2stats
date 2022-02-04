#!/usr/bin/env python3
""" Update data/players.yaml with new information."""

from datetime import datetime, timedelta
import os

import requests
import yaml

SE_PLAYERS = "https://raw.githubusercontent.com/SiegeEngineers/aoc-reference-data/master/data/players.yaml"

DATA_FILE = "data/players.yaml"

def save_yaml(players):
    with open(DATA_FILE, "w") as f:
        yaml.dump(players, f)

def local_player_yaml():
    with open(DATA_FILE) as f:
        return yaml.safe_load(f)
    
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
        found = False
        for player in players:
            if remote_player["id"] == player["id"]:
                found = True
                break
        if found:
            for key, attr in remote_player.items():
                player[key] = attr
        else:
            players.append(remote_player)
    with open(DATA_FILE, "w") as f:
        yaml.dump(players, f)

def run():
    pass

if __name__ == "__main__":
    run()
