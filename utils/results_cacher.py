#!/usr/bin/env python
""" Save weekly calculations of popularity, win rates, rank, and sample_size."""

from collections import defaultdict
from datetime import datetime, timezone
import psycopg2

import statistics
from statsmodels.stats.proportion import proportion_confint

from utils.models import Player
from utils.tools import all_wednesdays, batch, DB, SEVEN_DAYS_OF_SECONDS
from utils.tools import execute_sql, execute_bulk_insert, execute_transaction

DBS = {"current": DB}


def db_path():
    """ The path to the database."""
    return DBS["current"]


CREATE_RESULTS_TABLE = """CREATE TABLE IF NOT EXISTS results (
id integer PRIMARY KEY,
week text,
civ_id text,
team_size integer,
map_category text,
methodology text,
metric text,
compound integer,
rank integer,
pct real,
sample_size integer,
UNIQUE(week, civ_id, team_size, map_category, methodology, metric, compound))"""

CREATE_WEEKCOUNTS_TABLE = """CREATE TABLE IF NOT EXISTS week_counts (
id integer PRIMARY KEY,
week text,
match_count integer,
UNIQUE(week))"""

WEEK_COUNT_SQL_TEMPLATE = """ SELECT match_count FROM week_counts
WHERE week = '{}' """

MATCHES_SQL_TEMPLATE = """SELECT count(*) FROM matches
                          WHERE started BETWEEN {:0.0f} AND {:0.0f}"""
QUERIES = {
    "match_popularity": """SELECT civ, COUNT(*) AS cnt FROM
                           (SELECT STRING_AGG(CAST(civ_id AS TEXT), ':') AS civ FROM
                            (SELECT match_id, civ_id, won
                             FROM matches
                             WHERE civ_id IS NOT NULL
                             AND started BETWEEN {:0.0f} AND {:0.0f}
                             {}
                             ORDER BY match_id, won, civ_id) AS t1
                            GROUP BY match_id, won) AS t2
                           GROUP BY civ""",
    "player_popularity": """SELECT player, civ, COUNT(*) AS cnt FROM
                            (SELECT STRING_AGG(CAST(player_id AS TEXT), ':') as player,
                             STRING_AGG(CAST(civ_id AS TEXT), ':') AS civ FROM
                             (SELECT player_id, match_id, civ_id, won
                              FROM matches
                              WHERE civ_id IS NOT NULL
                              AND started BETWEEN {:0.0f} AND {:0.0f}
                              {}
                              ORDER BY player_id, won, civ_id) AS t1
                             GROUP BY match_id, won) AS t2
                            GROUP BY player, civ""",
    "match_popularity_basic": """SELECT cast(civ_id as text), COUNT(*) AS cnt FROM matches
                                 WHERE civ_id IS NOT NULL
                                 AND started BETWEEN {:0.0f} AND {:0.0f}
                                 {}
                                 GROUP BY civ_id""",
    "player_popularity_basic": """SELECT cast(player_id as text), cast(civ_id as text),
                                  COUNT(*) AS cnt FROM matches
                                  WHERE civ_id IS NOT NULL
                                  AND started BETWEEN {:0.0f} AND {:0.0f}
                                  {}
                                  GROUP BY player_id, civ_id""",
    "win_rates_match": """ SELECT civ_id, won, COUNT(*) as cnt FROM matches
                           WHERE civ_id IS NOT NULL
                           AND mirror = false
                           AND started BETWEEN {:0.0f} AND {:0.0f}
                           {}
                           GROUP BY civ_id, won""",
    "win_rates_player": """ SELECT civ_id, AVG(CAST(won AS INT)) FROM matches
                            WHERE civ_id IS NOT NULL
                            AND mirror = false
                            AND started BETWEEN {:0.0f} AND {:0.0f}
                            {}
                            GROUP BY player_id, civ_id""",
}


class CivDict(defaultdict):
    """ Generates civ if missing."""

    def __init__(self, klass, size, map_category, methodology):
        super().__init__()
        self.klass = klass
        self.size = size
        self.map_category = map_category
        self.methodology = methodology

    def __missing__(self, key):
        str_key = str(key)
        if str_key in self:
            return self[str_key]
        civ_ids = []

        for i in str_key.split(":"):
            civ_ids.append(i)
        self[str_key] = self.klass(
            ":".join(civ_ids), self.size, self.map_category, self.methodology
        )
        return self[str_key]


def build_category_filters():
    """ Generate category_filters because easier."""
    category_filters = {}
    for name, condition in (("1v1", "= 1",), ("2v2", "= 2",), ("team", "> 1",)):
        category_filters[
            "All {}".format(name)
        ] = "AND game_type = 0 AND team_size {}".format(condition)
        category_filters[
            "Arabia {}".format(name)
        ] = "AND game_type = 0 AND team_size {} AND map_type = 9".format(condition)

        category_filters[
            "Arena {}".format(name)
        ] = "AND game_type = 0 AND team_size {} AND map_type = 29".format(condition)
        category_filters[
            "Others {}".format(name)
        ] = "AND game_type = 0 AND team_size {} AND map_type NOT IN (9,29)".format(
            condition
        )
    return category_filters


CATEGORY_FILTERS = build_category_filters()


class Civilization:
    """ Base class for holder of weekly information."""

    def __init__(self, civ_id, size, map_category, methodology):
        self.civ_id = civ_id
        self.size = size
        self.map_category = map_category
        self.methodology = methodology
        self.rank = 0
        self.total = 0

    @property
    def pct(self):
        """ Override in subclass."""
        raise NotImplementedError

    @property
    def metric(self):
        """ Override in subclass."""
        raise NotImplementedError

    @property
    def sample_size(self):
        """ Override in subclass."""
        raise NotImplementedError

    @property
    def compound(self):
        """ Override in subclass."""
        raise NotImplementedError

    def info(self):
        """ Information about civ."""
        return [
            self.civ_id,
            self.size,
            self.map_category,
            self.methodology,
            self.metric,
            self.compound,
            self.rank,
            self.pct,
        ]


class PopularCivilization(Civilization):
    """ Holds weekly popularity information. """

    def __init__(self, civ_id, size, map_category, methodology):
        Civilization.__init__(self, civ_id, size, map_category, methodology)
        self.times_used = 0.0
        self.total = 0.0
        self.rank = 0

    @property
    def score(self):
        """ How to rank"""
        return self.times_used

    @property
    def compound(self):
        """ Whether a single civ or a set of civs."""
        return ":" in self.civ_id

    @property
    def pct(self):
        """ Percentage of plays in this category this week. """
        return round(self.times_used / (self.total or 1.0), 3)

    @property
    def metric(self):
        """ What is actually measured."""
        return "popularity"

    @property
    def sample_size(self):
        """ How many data points."""
        return self.times_used


class WinrateCivilization(Civilization):
    """ Holds weekly winrate information. """

    def __init__(self, civ_id, size, map_category, methodology):
        Civilization.__init__(self, civ_id, size, map_category, methodology)
        self.win_results = []
        self.cached_winrate_pct = None

    def __str__(self):
        return "{:2}: {:.3f}".format(self.civ_id, self.pct)

    @property
    def sample_size(self):
        """ How many data points."""
        return len(self.win_results)

    @property
    def metric(self):
        """ What is actually measured."""
        return "winrate"

    @property
    def score(self):
        """ How to rank"""
        return self.pct

    @property
    def compound(self):
        """ Whether a single civ or a set of civs."""
        return False

    @property
    def pct(self):
        """ Percentage of games won by this civ. """
        if self.cached_winrate_pct is None:
            try:
                self.cached_winrate_pct = round(statistics.mean(self.win_results), 3)
            except statistics.StatisticsError:
                self.cached_winrate_pct = 0
        return self.cached_winrate_pct


class BottomWinrateCivilization(WinrateCivilization):
    """ Holds weekly bottom winrate information."""

    @property
    def metric(self):
        """ What is actually measured."""
        return "bottom_winrate"

    @property
    def pct(self):
        """ Percentage of games won by this civ assuming the worst. """
        if self.cached_winrate_pct is None:
            try:
                wins = sum(self.win_results)
                total = len(self.win_results)
                low_confidence, _ = proportion_confint(wins, total)
                self.cached_winrate_pct = round(low_confidence, 3)
            except statistics.StatisticsError:
                self.cached_winrate_pct = 0
        return self.cached_winrate_pct


def filters(category, size):
    """ Generates additional "where" conditions for query """
    return CATEGORY_FILTERS["{} {}".format(category, size)]


def most_popular_match(timebox, size, map_category, basic):
    """ Returns civs with most popular by match for given week. """
    if not basic and size != "2v2":
        return []
    key = "match_popularity_basic" if basic else "match_popularity"
    sql = QUERIES[key].format(*timebox, filters(map_category, size))
    total = 0
    civs = CivDict(PopularCivilization, size, map_category, "match")
    for civ_id, count in execute_sql(sql, db_path()):
        total += count
        civ = civs[civ_id]
        civ.times_used += count

    def civ_sorter(civ):
        return -1 * civ.times_used

    rank_civs(civs.values(), civ_sorter, total)

    return list(civs.values())


def most_popular_player(timebox, size, map_category, basic):
    """ Returns civs with most popular by player for given week. """
    if not basic and size != "2v2":
        return []
    key = "player_popularity_basic" if basic else "player_popularity"
    sql = QUERIES[key].format(*timebox, filters(map_category, size))
    players = defaultdict(Player)
    for player_id, civ_id, count in execute_sql(sql, db_path()):
        pair = [int(x) for x in str(player_id).split(":")]
        sorted_player_id = ":".join([str(x) for x in sorted(pair)])
        players[sorted_player_id].add_civ_use(civ_id, count)

    civs = CivDict(PopularCivilization, size, map_category, "player")
    for player in players.values():
        for civ_id, value in player.civ_preference_units.items():
            civ = civs[civ_id]
            civ.times_used += value

    def civ_sorter(civ):
        return -1 * civ.times_used

    rank_civs(civs.values(), civ_sorter, len(players))

    return list(civs.values())


def rank_civs(civs, sorter, total):
    """ Set rank and total for each civ."""
    last_rank = 0
    last_score = 0

    for rank, civ in enumerate(sorted(civs, key=sorter), 1):
        score = civ.score
        if score == last_score:
            civ.rank = last_rank
        else:
            civ.rank = rank
            last_rank = rank
            last_score = score
        civ.total = total


def winrate_match(timebox, size, map_category):
    """ Returns civs with winrate data based on matches. """
    sql = QUERIES["win_rates_match"].format(*timebox, filters(map_category, size))
    civs = CivDict(WinrateCivilization, size, map_category, "match")
    bottom_civs = CivDict(BottomWinrateCivilization, size, map_category, "match")
    total = 0
    for civ_id, won, count in execute_sql(sql, db_path()):
        total += count
        wins = [won for _ in range(count)]
        civ = civs[civ_id]
        civ.win_results += wins
        bottom_civ = bottom_civs[civ_id]
        bottom_civ.win_results += wins

    def sort_win_pct(civ):
        return -1 * civ.pct

    rank_civs(civs.values(), sort_win_pct, total)
    rank_civs(bottom_civs.values(), sort_win_pct, total)

    return list(civs.values()) + list(bottom_civs.values())


def winrate_player(timebox, size, map_category):
    """ Returns civs with winrate data based on player percentage. """
    sql = QUERIES["win_rates_player"].format(*timebox, filters(map_category, size))
    civs = CivDict(WinrateCivilization, size, map_category, "player")

    total = 0
    for civ_id, won_avg in execute_sql(sql, db_path()):
        total += 1
        civ = civs[civ_id]
        civ.win_results.append(won_avg)

    def sort_win_pct(civ):
        return -1 * civ.pct

    rank_civs(civs.values(), sort_win_pct, total)
    return list(civs.values())


def timeboxes_to_update():
    """ Returns array of timebox tuples that need to be updated."""
    timeboxes = []
    for wednesday in all_wednesdays():
        old_count = None
        week = wednesday.strftime("%Y%m%d")
        for (count,) in execute_sql(WEEK_COUNT_SQL_TEMPLATE.format(week), db_path()):
            old_count = count
        timestamp = datetime.timestamp(wednesday)
        timebox = (
            timestamp,
            timestamp + SEVEN_DAYS_OF_SECONDS,
        )
        if old_count:
            new_count = None
            for (count,) in execute_sql(
                MATCHES_SQL_TEMPLATE.format(*timebox), db_path()
            ):
                new_count = count
            if old_count == new_count:
                print("Skipping", week)
                continue

        timeboxes.append(timebox)
    return timeboxes


def save_civs(civs, timebox):
    """ Calls saves on the civs and updates week_counts."""
    results_sql = """INSERT INTO results
    (week, civ_id, team_size, map_category, methodology, metric,
    compound, rank, pct)
    VALUES %s
ON CONFLICT (week, civ_id, team_size, map_category, methodology, metric, compound) DO UPDATE SET rank=Excluded.rank, pct=Excluded.pct"""
    wednesday = datetime.fromtimestamp(timebox[0], tz=timezone.utc)
    week = wednesday.strftime("%Y%m%d")
    print("Saving", week, len(civs))
    for match_batch in batch(civs, 300):
        civ_batch = [[week] + civ.info() for civ in match_batch]
        execute_bulk_insert(results_sql, civ_batch)
    match_count = None
    for (count,) in execute_sql(MATCHES_SQL_TEMPLATE.format(*timebox), db_path()):
        match_count = count
    week_counts_sql = """INSERT INTO week_counts
    (week, match_count) VALUES (%s, %s)
    ON CONFLICT (week) DO UPDATE SET match_count=EXCLUDED.match_count"""
    execute_transaction(week_counts_sql, (week, match_count))


def generate_results():
    """ Generate all the results"""
    categories = {x.split()[0] for x in CATEGORY_FILTERS}
    for timebox in timeboxes_to_update():
        wednesday = datetime.fromtimestamp(timebox[0], tz=timezone.utc)
        print("Generating Results for {}".format(wednesday.strftime("%Y%m%d")))
        civs = []
        for map_category in categories:
            for team_size in ("1v1", "2v2", "team"):
                print("  {} {}".format(team_size, map_category))
                civs.extend(most_popular_match(timebox, team_size, map_category, True))
                civs.extend(most_popular_match(timebox, team_size, map_category, False))
                civs.extend(winrate_match(timebox, team_size, map_category))
                civs.extend(most_popular_player(timebox, team_size, map_category, True))
                civs.extend(
                    most_popular_player(timebox, team_size, map_category, False)
                )
                civs.extend(winrate_player(timebox, team_size, map_category))
        save_civs(civs, timebox)


def run():
    """ Basic functioning of app."""
    generate_results()


if __name__ == "__main__":
    run()
