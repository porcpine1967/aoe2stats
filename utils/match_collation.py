#!/usr/bin/env python
""" Builds matches from json."""
from datetime import timedelta
import json

from utils.update import validate_player_info, PlayerInfoException


class Rating:
    """ Holds rating-change information."""

    def __init__(self, blob):
        self.won = blob["streak"] > 0
        self.rating = blob["rating"]
        self.timestamp = blob["timestamp"]

    def before(self, match):
        """ If rating was assigned before the match."""
        return self.timestamp < match.started

    def after(self, match):
        """ If rating was assigned after the match."""
        return self.timestamp > match.started


def timestamp_sort(obj):
    """ Sort key for rating."""
    return obj.timestamp


class Match:
    """ Holds match information."""

    def __init__(self, blob):
        self.match_id = blob["match_id"]
        self.version = blob["version"]
        self.map_type = blob["map_type"]
        self.rating_type = blob["rating_type_id"]
        self.started = blob["started"]
        self.player_ids = (
            blob["players"][0]["profile_id"],
            blob["players"][1]["profile_id"],
        )
        self.players = {}
        civs = set()
        for player in blob["players"]:
            self.players[player["profile_id"]] = {
                "civ_id": player["civ"],
                "rating": player["rating"],
            }
            civs.add(player["civ"])
        self.mirror = len(civs) == 1
        self._rating = None
        self.finished = None

    def add_rating(self, rating):
        """ Sets the rating change for the match."""
        if self._rating:
            raise MatchValidityError("already has rating")
        self._rating = rating
        self.finished = rating.timestamp

    @property
    def _base_row(self):
        return [
            self.match_id,
            self.map_type,
            self.rating_type,
            self.version,
            self.started,
            self.finished,
            1,
            self.rating_type,
        ]

    def match_rows(self, context_profile_id):
        """ Returns rows for db insertion.
context_profile_id for who won or lost vis a vis the rating."""
        rows = []
        for profile_id, data in self.players.items():
            row = self._base_row
            row.append(profile_id)
            row.append(data["civ_id"])
            if profile_id == context_profile_id:
                row.append(self._rating.rating)
                row.append(self._rating.won)
            else:
                row.append(data["rating"])
                row.append(not self._rating.won)
            row.append(self.mirror)

            rows.append(row)
        return rows

    @property
    def valid(self):
        """ If the match is valid."""
        return bool(self._rating)

    @property
    def duration(self):
        """ How long the match lasted."""
        return timedelta(seconds=self.finished - self.started)


class MatchValidityError(Exception):
    pass


class MatchCollator:
    """ Collates the ratings and matches."""

    def __init__(self, profile_id):
        self.profile_id = profile_id
        self.matches = []
        self.ratings = []

    def collate(self, matches_blob, ratings_blob):
        """ Put all the ratings in the matches."""
        ratings = []
        for blob in ratings_blob:
            ratings.append(Rating(blob))
        for blob in matches_blob:
            if blob["name"] == "AUTOMATCH":
                try:
                    validate_player_info(blob, False)
                    self.matches.append(Match(blob))
                except PlayerInfoException:
                    pass
        self.ratings = sorted(ratings, key=timestamp_sort)

        for match in self.matches:
            for rating in self.ratings:
                if rating.after(match):
                    match.add_rating(rating)
                    break

    @property
    def match_rows(self):
        """ All the matches for insertion in the db."""
        rows = []
        for match in self.matches:
            if match.valid:
                rows.extend(match.match_rows(self.profile_id))
        return rows


def run():
    """ doit """
    with open("tmp/m.json") as fhandle:
        matches_blob = json.load(fhandle)
    with open("tmp/rh.json") as fhandle:
        ratings_blob = json.load(fhandle)

    mb = MatchCollator(5486218)
    mb.collate(matches_blob, ratings_blob)


if __name__ == "__main__":
    run()
