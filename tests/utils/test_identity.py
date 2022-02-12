#!/usr/bin/env python3

import pytest

from utils.identity import canonical_identifiers, player_yaml

def test_canonical_identifiers():
    players = player_yaml()
    assert ('noboru', '/ageofempires/Noboru43',) == canonical_identifiers('Noboru43', '/ageofempires/noboru', players)
    assert ('TheViper', '/ageofempires/TheViper',) == canonical_identifiers('The_Viper', '/ageofempires/TheViper', players)
    assert ('foo', None,) == canonical_identifiers('foo', None, players)
    assert players == player_yaml()
    assert ('BlackheartB', '/ageofempires/Boobie',) == canonical_identifiers('BlackheartB', '/ageofempires/Boobie', players)
    assert players != player_yaml()
    for player in players:
        if player['canonical_name'] == 'BlackheartB':
            assert player['liquipedia'] == 'Boobie'
            break
    else:
        pytest.fail("did not find BlackheartB")
