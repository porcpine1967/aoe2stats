#!/usr/bin/env python3
""" Grabs data from robo-atp spreadsheet """

from collections import defaultdict
import csv
from datetime import date, timedelta
import json
import os

from dateutil.relativedelta import relativedelta
import requests

from utils.robo_atp_loader import ROBO
from utils.tools import cache_file

ROBO_FILE = "cache/roboatp.json"
REQUEST_HEADER = {'x-referer': 'https://explorer.apis.google.com'}
REQUEST_URL = 'https://content-sheets.googleapis.com/v4/spreadsheets/1HaBkO10i51rqxWytJwfBCgPdISp_cdXzSKyXIPT5FTY/values/Tourney+Results!A:H?key=AIzaSyAa8yy0GdcGPHdtD083HiGGx_S0vMPScDM'
LINKS_URL = 'https://content-sheets.googleapis.com/v4/spreadsheets/1HaBkO10i51rqxWytJwfBCgPdISp_cdXzSKyXIPT5FTY/values/Tourney+Results!A:H?key=AIzaSyAa8yy0GdcGPHdtD083HiGGx_S0vMPScDM'

def robo_data():
    """ Fetches (cached) tournament results """
    cache_file(ROBO_FILE, REQUEST_URL, REQUEST_HEADER)
    with open(ROBO_FILE) as f:
        return json.load(f)

def player_ratings(end_date):
    """ Returns player-name:rating data for players on a given day """
    start_date = end_date - timedelta(days=365)
    ratings = defaultdict(int)
    for row in robo_data()['values'][1:]:
        try:
            tourney_date = date.fromisoformat(row[0].replace('/', '-'))
            if start_date < tourney_date < end_date:
                ratings[row[5]] += float(row[7])
        except ValueError:
            pass
    return ratings

def links():
    

def run():
    pass
if __name__ == '__main__':
    run()
