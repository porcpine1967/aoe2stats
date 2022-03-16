#!/usr/bin/env python3
""" Gathers information on tournaments from podcast texts."""
from argparse import ArgumentParser
from collections import defaultdict
import json
import os
import re

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import TournamentManager


PODCAST_PATTERN = re.compile("2.*txt$")

class Podcast:
    def __init__(self, path):
        self.paragraphs = []
        last_paragraph = ""
        with open(path) as f:
            for line in f:
                if line == "\n":
                    self.paragraphs.append(last_paragraph)
                    last_paragraph = ""
                else:
                    last_paragraph += line
        self.paragraphs.append(last_paragraph)

def podcasts(podcast_dir):
    podcast_holder = []
    for root, _, files in os.walk(podcast_dir):
        for filename in files:
            if PODCAST_PATTERN.match(filename):
                podcast_holder.append(Podcast("{}/{}".format(root, filename)))
    return podcast_holder
        
def tournament_mentions(podcast_dir, tournament_names):
    tournaments = defaultdict(list)
    for podcast in podcasts(podcast_dir):
        for tournament_name in tournament_names:
            for paragraph in podcast.paragraphs:
                if tournament_name in paragraph:
                    tournaments[tournament_name].append(paragraph)
    return tournaments

def arguments():
    parser = ArgumentParser()
    parser.add_argument("podcasts", help="directory that holds podcasts")
    return parser.parse_args()

def run():
    tournament_names = []
    manager = TournamentManager(Loader())
    for tournament in manager._tournaments:
        tournament_names.append(tournament.name)
    tournaments = tournament_mentions(arguments().podcasts, tournament_names)
    with open('cache/tournament_mentions.json', 'w') as f:
        json.dump(tournaments, f)

if __name__ == '__main__':
    run()
