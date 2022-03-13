#!/usr/bin/env python3
""" Does odd things needed for seeding """

from collections import defaultdict
import json

import requests

API_TEMPLATE = "https://aoe2.net/api/leaderboard?game=aoe2de&leaderboard_id={leaderboard}&start=1&count=120&search={name}"

def user_search(name, leaderboard):
    """ Searches leaderboard from aoe2.net for name """
    try:
        url = API_TEMPLATE.format(name=name, leaderboard=leaderboard)
        response = requests.get(url)
        if response.status_code != 200:
            return defaultdict(lambda: "UNKNOWN")
        return json.loads(response.text)
    except:
        return defaultdict(lambda: "UNKNOWN")

YAML = """
- canonical_name: {canonname}
  country: {country}
  name: {name}
  platforms:
    rl:
    - '{profile_id}'

"""
"""
PhillipHJS, Rooster, Jerry
"""
alt_users = {'Imperio_CC', 'Forastero', 'Mo_Tastic', 'LemonJack', 'Fresh To Death', 'c3mbre', 'Jimmy2Timez', 'INeedMoney', 'S3Kingcat', 'mratin', 'Leisure_', 'Cong4ever', 'Pro_Tastic', 'roggy', 'Mununez', 'LoneStarr', 'Bullet', 'IamZak', 'DoubleZX_', 'NakedNipples', 'PleaseBeGentle', 'Professor4k', 'Concur', 'Zastosy', 'Blink182', 'NoLo', 'PushTheTempo', 'kylar_', 'bobdedestructor', 'MrPlanner', 'silverstar', 'teutonic_tanks', 'Rooster', 'Roggy', 'EmpathyRTS', 'Slaktarn', '', 'Locuss', 'Target331', 'PraYs', 'Oli King', 'Jerry', 'yanki', 'Rgeadn', 'Matt107', 'Glokken', 'Vilager', 'Kedaxx', 'AncientSmokealot', 'Lagosta', 'hehej0', 'Rxndy', 'Imperio_cc', 'Nolofinwe'}
def run():
    users = ('Jerry', )
    for username in users:
        found = False
        for user in user_search(username, 4)['leaderboard']:
            if not user.get('country') == 'US':
                continue
            print(user)
            found = True
            print(YAML.format(canonname=username, country=user['country'].lower(), name=user['name'], profile_id=user['profile_id']))
        if not found:
            for user in user_search(username, 3)['leaderboard']:
                found = True
                print(YAML.format(canonname=username, country=user['country'].lower(), name=user['name'], profile_id=user['profile_id']))
            
        break
if __name__ == '__main__':
    run()
