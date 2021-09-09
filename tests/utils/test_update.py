#!/usr/bin/env python
""" Tests utilities in update module. """
import pytest

from utils.update import deduce_rating_type


def test_rating_type():
    """ Make sure returns appropriate rating type. """
    ranked_match = {"rating_type": 2}
    assert deduce_rating_type(ranked_match) == 2
    random = {"rating_type": 0, "game_type": 0, "num_players": 2}
    assert deduce_rating_type(random) == 2
    team_random = {"rating_type": 0, "game_type": 0, "num_players": 6}
    assert deduce_rating_type(team_random) == 4
    death_match = {"rating_type": 0, "game_type": 2, "num_players": 2}
    assert deduce_rating_type(death_match) == 1
    team_death_match = {"rating_type": 0, "game_type": 2, "num_players": 6}
    assert deduce_rating_type(team_death_match) == 3
    battle_royale = {"rating_type": 0, "game_type": 12, "num_players": 2}
    assert deduce_rating_type(battle_royale) == 9
    team_battle_royale = {"rating_type": 0, "game_type": 12, "num_players": 6}
    assert deduce_rating_type(team_battle_royale) == 9
    empire_wars = {"rating_type": 0, "game_type": 13, "num_players": 2}
    assert deduce_rating_type(empire_wars) == 13
    team_empire_wars = {"rating_type": 0, "game_type": 13, "num_players": 6}
    assert deduce_rating_type(team_empire_wars) == 14
    co_op = {"rating_type": 0, "game_type": 15, "num_players": 6}
    assert deduce_rating_type(co_op) is None
