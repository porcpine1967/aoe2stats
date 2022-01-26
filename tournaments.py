#!/usr/bin/env python3
""" Writes tournament information for week to file."""
from datetime import date, datetime, timedelta
from pathlib import Path
import os.path
import sys

from liquiaoe.loaders import HttpsLoader
from liquiaoe.managers.tournament_manager import TournamentManager


from utils.tools import last_time_breakpoint

LOADER = HttpsLoader()
GAMES = ("Age of Empires II", "Age of Empires IV",)
TIERS = ("S-Tier", "A-Tier", "B-Tier",)

def timeboxes():
    breakpoint = last_time_breakpoint(date(2022,1,26)).date()
    return (
        (breakpoint - timedelta(days=7), breakpoint - timedelta(days=1),),
        (breakpoint, breakpoint + timedelta(days=6),),
        )

def completed_tournament_lines(tournament, complete):
    lines = []
    if "/Winner_Stays_On/" in tournament.url:
        return lines
    tournament.load_advanced(LOADER)
    lines.append(tournament.name)
    lines.append("  " + tournament.description)
    lines.append("  url: https://liquipedia.net{}".format(tournament.url))
    if tournament.series:
        lines.append("  Series: {}".format(tournament.series))
    lines.append("  Format: {}".format(tournament.format_style))
    if tournament.organizers:
        lines.append("  Organizer: {}".format(", ".join(tournament.organizers)))
    if tournament.sponsors:
        lines.append("  Sponsor: {}".format(", ".join(tournament.sponsors)))
    lines.append("  Tier (prize pool): {} ({})".format(tournament.tier, tournament.prize))
    if complete:
        lines.append("  Winners:")
        first_place_url = "https://liquipedia.net{}/Results".format(tournament.first_place_url)
        lines.append("    First:  {} ({})".format(tournament.first_place, first_place_url))
        lines.append("    Second: {}".format(tournament.second_place))
        if len(tournament.runners_up) == 1:
            lines.append("    3rd-4th: {}".format(tournament.runners_up[0]))
        if len(tournament.runners_up) == 2:
            lines.append("    Third: {}".format(tournament.runners_up[0]))
            lines.append("    Fourth: {}".format(tournament.runners_up[1]))
    else:
        if tournament.start == tournament.end:
            lines.append("  Date: {}".format(tournament.start.strftime("%a %b %d")))
        else:
            lines.append("  Dates: {} - {}".format(tournament.start.strftime("%a %b %d"),
                                                   tournament.end.strftime("%a %b %d")))
    lines.append("")
    return lines

def print_info(tournament_dict, complete):
    lines = []
    for game, tournaments in tournament_dict.items():
        if not game in GAMES:
            continue
        lines.append("*"*25)
        lines.append(game)
        lines.append("*"*25)
        for tournament in tournaments:
            if tournament.tier in TIERS:
                lines.extend(completed_tournament_lines(tournament, complete))
        lines.append("")
    return lines

def setup_and_verify(working_dir):
    """ Make an appropriate directory. Don't run if file already there."""
    working_file = "{}/tournament_info.txt".format(working_dir)
    if os.path.exists(working_file):
        return False
    Path(working_dir).mkdir(exist_ok=True)
    return working_file
    
def run():
    """ Do the thing"""
    last_week, this_week = timeboxes()
    working_dir = this_week[0].strftime("%Y%m%d")
    working_file = setup_and_verify(working_dir)
    if not working_file:
        print("Already done this week.")
        sys.exit(0)
    manager = TournamentManager(LOADER)
    lines = []
    lines.append("="*25)
    lines.append("COMPLETED")
    lines.append("="*25)
    lines.extend(print_info(manager.completed(last_week), True))
    lines.append("="*25)
    lines.append("ONGOING")
    lines.append("="*25)
    lines.extend(print_info(manager.ongoing(this_week), False))
    lines.append("="*25)
    lines.append("STARTING")
    lines.append("="*25)
    lines.extend(print_info(manager.starting(this_week), False))
    with open(working_file, "w") as f:
        for line in lines:
            print(line)
            f.write(line)
            f.write("\n")
    
if __name__ == "__main__":
    run()
