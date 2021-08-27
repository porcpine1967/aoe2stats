#!/usr/bin/env python
""" Model classes. """
from collections import Counter, defaultdict


class Player:
    """ Object to calculate player-preference-units. """

    def __init__(self):
        self.civ_uses = Counter()
        self.map_uses = Counter()
        self.civ_wins = defaultdict(dict)
        self.wins = Counter()
        self.total = 0.0

    def add_civ_use(self, civ, civ_count):
        """ Adds civilization usage data for later calculations. """
        self.civ_uses[civ] += civ_count
        self.total += civ_count

    def add_map_use(self, map_type, map_count):
        """ Adds map usage data for later calculations. """
        self.map_uses[map_type] += map_count
        self.total += map_count

    def add_civ_win(self, civ, won, win_count):
        """ Adds civilization win data for later calculations. """
        self.civ_wins[civ][won] = win_count
        self.wins[won] += win_count

    @property
    def civ_preference_units(self):
        """ Generates civ preference units of player. """
        pus = {}
        for civ, total in self.civ_uses.items():
            pus[civ] = total / self.total
        return pus

    @property
    def map_preference_units(self):
        """ Generates map preference units of player. """
        pus = {}
        for map_type, total in self.map_uses.items():
            pus[map_type] = total / self.total
        return pus

    def win_percentage(self, civ):
        """ Calculates the user's win percentage for the given civ."""
        data = self.civ_wins[civ]
        if 1 in data and 0 not in data:
            return 1
        if 1 not in data and 0 in data:
            return 0
        win_count = data[1]
        total = data[0] + data[1]
        return float(win_count) / total
