#!/usr/bin/env python3
""" Loads robo-atp data into scores.

1. Save a copy of https://docs.google.com/spreadsheets/d/1HaBkO10i51rqxWytJwfBCgPdISp_cdXzSKyXIPT5FTY/edit?pli=1#gid=1779898269
2. Set 'Tourney Results' P1:
=DATE(YEAR(R1)-1, MONTH(R1), DAY(R1))
3. Set 'Tourney Results' R1 to {tournament-start-date}
4. Sort 'Player Ranks' K3 Z-A
5. Copy columns 'Player Ranks' E-K
6. Paste into tmp/robo-{tournament-start-date}
"""
from argparse import ArgumentParser
import csv
from datetime import date, datetime
import logging
import os
import re

from utils.tools import execute_bulk_insert, player_yaml
from utils.tools import LOGGER_NAME, setup_logging

SAVE_SCORES_SQL = """INSERT INTO scores
(evaluation_date, player_url, score, scorer)
VALUES %s
ON CONFLICT DO NOTHING
"""

LOGGER = logging.getLogger(LOGGER_NAME)

SCORER = 'robo-atp'

ROBO = re.compile(r'^robo-(.{10})$')

def alias_lookup():
    lookup = dict()
    for player in player_yaml():
        lookup[player['name'].lower()] = player
        try:
            lookup[player['liquipedia'].lower()] = player
        except KeyError:
            pass
        try:
            for alias in player['aka']:
                lookup[alias.lower()] = player                
        except KeyError:
            pass
    return lookup

def rows_for_file(filename, player_lookup):
    m = ROBO.match(filename)
    if not m:
        return list()
    evaluation_date = datetime.strptime(m.group(1), '%Y-%m-%d').date()
    rows = []
    with open("tmp/{}".format(filename)) as f:
        data = csv.reader(f, delimiter="\t")
        for row in data:
            name = row[0]
            if not name:
                continue
            try:
                score = int(row[6])
                player = player_lookup[name.lower()]
            except ValueError:
                continue
            except KeyError:
                if score > 0:
                    LOGGER.warning("Unable to find {}".format(name))
                continue
            if not 'liquipedia' in player:
                continue
            player_url = "/ageofempires/{}".format(player['liquipedia'])
            rows.append((evaluation_date, player_url, score, SCORER,))
    return rows

def update_players():
    player_lookup = alias_lookup()
    rows = []
    for filename in os.listdir('tmp'):
        rows.extend(rows_for_file(filename, player_lookup))
        

    if rows:
        execute_bulk_insert(SAVE_SCORES_SQL, rows)

def arguments():
    parser = ArgumentParser()
    parser.add_argument('--debug', action='store_true', help="Set logger to debug")
    args = parser.parse_args()
    if args.debug:
        setup_logging(logging.DEBUG)
    else:
        setup_logging()
    return args

def run():
    arguments()
    update_players()

if __name__ == '__main__':
    run()
