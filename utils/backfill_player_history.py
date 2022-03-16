#!/usr/bin/env python3
""" Fills all ranked matches for given players"""
from datetime import datetime, timedelta
import json
import sys
import time

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from utils.aoe_elo_loader import aoe_elo_player_per_month
from utils.identity import player_yaml
from utils.tools import execute_sql
from utils.update import RANKED_DB, PlayerInfoException, save_matches, validate_player_info
from utils.versions import version_for_timestamp

MAX_DOWNLOAD = 1000

API_TEMPLATE = "https://aoe2.net/api/player/matches?game=aoe2de&profile_ids={profile_ids}&count={count}&start={start}"

def fetch_matches(player_url, matches, offset=0):
    print("LOADING {:15} ({:10})".format(player_url, offset))
    retry_strategy = Retry(backoff_factor=10, total=6)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    profile_ids = ",".join(PLAYERS[player_url].get('platforms', {}).get('rl', []))
    if not profile_ids:
        return matches
    url = API_TEMPLATE.format(start=offset, count=MAX_DOWNLOAD,profile_ids=profile_ids)
    response = http.get(url)
    if response.status_code != 200:
        print(response.text)
        sys.exit(1)
    data = json.loads(response.text)
    for match in data:
        # ignore if no map_type
        if not match["map_type"] or not match['ranked']:
            continue
        try:
            validate_player_info(match)
        except PlayerInfoException:
            continue
        match_rows = []

        row = [
            match["match_id"],
            match["map_type"],
            match["rating_type"],
            match["version"] or version_for_timestamp(match["started"]),
            match["started"],
            match["finished"],
            match["num_players"] / 2,
            match["game_type"],
        ]
        civs = set()
        for player in match["players"]:
            player_row = [
                player["profile_id"],
                player["civ"],
                player["rating"],
                player["won"] or False,
            ]
            match_rows.append(row + player_row)
            civs.add(player["civ"])

        # Determine if it is a mirror match (all have same civs)
        for row in match_rows:
            row.append(len(civs) == 1)

        matches += match_rows
    if len(data) >= MAX_DOWNLOAD:
        time.sleep(5)
        return fetch_matches(player_url, matches, offset + MAX_DOWNLOAD)
    return matches


def top_20_players():
    players = set()
    for player_list in aoe_elo_player_per_month().values():
        players.update(player_list)
    return players
        
def run():
    for player in top_20_players():
        matches = list()
        fetch_matches(player, matches)
        save_matches(matches, RANKED_DB)

if __name__ == '__main__':
    PLAYERS = {}
    for player in player_yaml():
        PLAYERS[player.get('liquipedia')] = player
    run()
