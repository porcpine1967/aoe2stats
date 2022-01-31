#!/usr/bin/env python3
""" Writes tournament information for week to file."""
from datetime import date, datetime, timedelta
from pathlib import Path
import os.path

from utils.tournament_loader import arguments, TournamentLoader
from utils.tools import tournament_timeboxes

def completed_tournament_lines(tournament):
    lines = []
    if "/Winner_Stays_On/" in tournament.url:
        return lines
    lines.append(tournament.name)
    lines.append("  " + tournament.description)
    lines.append("  url: https://liquipedia.net{}".format(tournament.url))
    if tournament.series:
        lines.append("  Series: {}".format(tournament.series))
    lines.append("  Format: {}".format(tournament.format_style))
    if tournament.organizers:
        lines.append("  Organizer: {}".format(tournament.organizers))
    if tournament.sponsors:
        lines.append("  Sponsor: {}".format(tournament.sponsors))
    lines.append("  Tier (prize pool): {} ({})".format(tournament.tier, tournament.prize))
    if tournament.first_place:
        lines.append("  Winners:")
        if tournament.first_place_url:
            first_place_url = "(https://liquipedia.net{}/Results)".format(tournament.first_place_url)
        else:
            first_place_url = ""
        lines.append("    First:  {} {}".format(tournament.first_place, first_place_url))
        lines.append("    Second: {}".format(tournament.second_place))
        if tournament.runners_up:
            runners_up = tournament.runners_up.split(', ')
            if len(runners_up) == 1:
                lines.append("    3rd-4th: {}".format(runners_up[0]))
            if len(runners_up) == 2:
                lines.append("    Third: {}".format(runners_up[0]))
                lines.append("    Fourth: {}".format(runners_up[1]))

        if tournament.first_place_tournaments:
            lines.append("    Tournament placements for {}".format(tournament.first_place))
        hold_year = datetime.now().year
        for fpt in tournament.first_place_tournaments:
            if fpt["name"] == tournament.name:
                continue
            if fpt["date"].year != hold_year:
                hold_year = fpt["date"].year
                lines.append("      {} {} {}".format("*"*25, hold_year, "*"*25))
            lines.append("      {}  {}  {:^9} {} {}".format(fpt["game"][-2:],
                                                            fpt["tier"][0],
                                                            fpt["place"],
                                                            fpt["date"].strftime("%b %d"),
                                                            fpt["name"]))
    else:
        if tournament.start == tournament.end:
            lines.append("  Date: {}".format(tournament.start.strftime("%a %b %d")))
        else:
            lines.append("  Dates: {} - {}".format(tournament.start.strftime("%a %b %d"),
                                                   tournament.end.strftime("%a %b %d")))
    lines.append("")
    return lines

def print_info(tournament_dict):
    lines = []
    for game, tournaments in tournament_dict.items():
        lines.append("*"*25)
        lines.append(game)
        lines.append("*"*25)
        for tournament in tournaments:
            lines.extend(completed_tournament_lines(tournament))
        lines.append("")
    return lines

def setup_and_verify(working_dir):
    """ Make an appropriate directory."""
    working_file = "{}/tournament_info.txt".format(working_dir)
    Path(working_dir).mkdir(exist_ok=True)
    return working_file
    
def run():
    """ Do the thing"""
    args = arguments()
    now = datetime.strptime(args.date, "%Y%m%d") if args.date else datetime.now()
    last_week, this_week = tournament_timeboxes(now)
    working_dir = this_week[0].strftime("%Y%m%d")
    working_file = setup_and_verify(working_dir)
    manager = TournamentLoader()
    lines = []
    lines.append("="*25)
    lines.append("COMPLETED")
    lines.append("="*25)
    lines.extend(print_info(manager.completed(last_week)))
    lines.append("="*25)
    lines.append("ONGOING")
    lines.append("="*25)
    lines.extend(print_info(manager.ongoing(this_week)))
    lines.append("="*25)
    lines.append("STARTING")
    lines.append("="*25)
    lines.extend(print_info(manager.starting(this_week)))
    with open(working_file, "w") as f:
        for line in lines:
            print(line)
            f.write(line)
            f.write("\n")
    
if __name__ == "__main__":
    run()
