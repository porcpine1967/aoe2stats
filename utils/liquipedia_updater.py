#!/usr/bin/env python3
import json
import time
import requests

from utils.identity import player_yaml, save_yaml
from utils.tools import setup_logging

LOGGER = setup_logging()

LINKS = "https://liquipedia.net/ageofempires/api.php?action=query&titles={}&prop=links|redirects&format=json"

class Normalizer:
    def __init__(self):
        self.last_call = 0
        self._headers = {"User-Agent": "liqui-aoe/0.1 (feroc.felix@gmail.com)","Accept-Encoding": "gzip"}

    def normalized_name(self, title):
        response = self._info(title)['query']
        name = title
        if 'normalized' in response:
            for item in response['normalized']:
                if item['from'] == title:
                    name = item['to'].replace(' ', '_')
        for page_id, data in response['pages'].items():
            if int(page_id) < 0:
                LOGGER.error("MISSING {}".format(title))
                return (None, [],)
            if len(data['links']) == 1:
                name = data['links'][0]['title'].replace(' ', '_')
                response = self._info(name)['query']

        return name, self.redirects(response)

    def redirects(self, response):
        try:
            for page_id, data in response['pages'].items():
                return [redirect['title'].replace(' ', '_') for redirect in data['redirects']]
        except KeyError:
            return []
        
    def _info(self, title):
        if self.last_call + 4 > time.time():
            time.sleep(4)
        url = LINKS.format(title)
        print("CALLING {}".format(title))
        response = requests.get(url, headers=self._headers)
        self.last_call = time.time()
        return response.json()

def run():
    n = Normalizer()
    players = player_yaml()
    for player in players:
        title = player.get('liquipedia')
        if not title:
            continue
        new_title, redirects = n.normalized_name(title)
        print(new_title, redirects)
        player['liquipedia'] = new_title
        if not new_title:
            del player['liquipedia']
        if redirects:
            player['liquipedia_redirects'] = redirects

    save_yaml(players)


if __name__ == '__main__':
    run()
