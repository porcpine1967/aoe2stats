#!/usr/bin/env python
""" Analyzes win rates based on game length."""
from collections import defaultdict


from statistics import mean, StatisticsError
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


from utils.tools import civ_map, execute_sql


class Match:
    """ Holds information about a match."""

    def __init__(self, row):
        self.match_id = row[0]
        self.started = row[1]
        self.finished = row[2]
        self.won = row[3]
        self.civ_id = row[4]
        self.rating = row[5] or 0

    @property
    def match_length(self):
        """ How many seconds the match lasted."""
        return self.finished - self.started

    @property
    def bucket(self):
        """ Which analytical bucket this match belongs in."""
        l = self.match_length
        if l < 776:  # 22 game minutes
            return 0
        if l < 1306:  # 37 game minutes
            return 1
        if l < 2118:  # 60 game minutes
            return 2
        return 3


class Player:
    """ Holds player matches."""

    def __init__(self):
        self.matches = []

    def win_average(self, civ_id, bucket):
        """ Returns the win average for this player
playing this civ in this bucket.

Throws StatisticsError if no match."""
        tally = []
        for match in self.matches:
            if match.civ_id == civ_id and match.bucket == bucket:
                tally.append(match.won)
        return mean(tally)

    @property
    def high_lever(self):
        for match in self.matches:
            if match.rating > 1650:
                return True
        return False


class Civ:
    """ Holds aggregate match information for a civ."""

    def __init__(self, civ_id):
        self.civ_id = civ_id
        self.matches = []
        self.players = set()

    def bucket_counts(self):
        """ Returns a dictionary of
win percentage of matches in each bucket."""
        buckets = defaultdict(list)
        for match in self.matches:
            buckets[match.bucket].append(match.won)
        avg = mean([m.won for m in self.matches])
        display = {}
        for name in ("SHORT", "MEDIUM", "LONG", "VERY LONG"):
            display[name] = int(1000 * (avg - mean(buckets[name])))
        return display

    def axes(self):
        """ Returns axes for plotting"""
        data = defaultdict(list)
        for player in self.players:
            for bucket in range(4):
                try:
                    average = player.win_average(self.civ_id, bucket)
                    data[bucket].append(average)
                except StatisticsError:
                    pass
        x_values = []
        y_values = []
        for i in range(4):
            x_values.append(i)
            y_values.append(mean(data[i]))

        return x_values, y_values, mean([m.won for m in self.matches])


def loaded_civs():
    """ Builds all the civs for later plotting."""
    cmap = civ_map()
    civs = {}
    players = defaultdict(Player)
    sql = """
    SELECT match_id, started, finished, won, civ_id, rating, player_id
FROM matches
WHERE game_type = 0
AND map_type = 9
AND team_size = 1
and started BETWEEN 1633406443 AND 1637110800
"""
    for row in execute_sql(sql):
        match = Match(row)
        # If it is less than 6 minutes, it was probably civ-unrelated
        # Dark Age shenanigans or rage quitting.
        if match.match_length < 353:
            continue
        name = cmap[row[4]]
        if name not in civs:
            civs[name] = Civ(row[4])
        player = players[row[6]]
        civs[name].players.add(player)
        player.matches.append(match)
        civs[name].matches.append(match)
    return civs


def all_civs_legend(legend):
    legend.set_title("Key")
    legend.yaxis.set_visible(False)
    legend.xaxis.set_visible(False)
    legend_elements = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            label="Win Rate",
            markerfacecolor="blue",
            markersize=7,
        ),
        Line2D([0], [0], color="red", lw=2, label="Average\nfor Civ"),
        Line2D([0], [0], color="lightgray", lw=2, label="50%", linestyle="dashed"),
    ]
    legend.legend(handles=legend_elements, frameon=False, loc="center")
    legend.annotate(
        """Win rates are calculated for games ending before 22 minutes,  between 22 and 37 minutes, 37 and 60 minutes, and over 60 minutes
Windows are 10% above and below average.""",
        xy=(-6, -0.3),
        xycoords="axes fraction",
        ha="center",
        va="center",
        fontsize=14,
    )


def plot_all_civs(civs, filename="tmp/glwr.png"):
    """ Plots a gamelengthwinrate chart with all civs on it."""
    fig, axs = plt.subplots(4, 10)
    fig.suptitle("Win Rates vs Game Length on Arabia\nAll Elos", fontsize=24)
    legend = axs[3, 9]
    all_civs_legend(legend)

    for index, name in enumerate(sorted(civs)):
        civ = civs[name]
        if index < 10:
            row = 0
        elif index < 20:
            row = 1
        elif index < 30:
            row = 2
        else:
            row = 3
        column = index - row * 10
        civ_axs = axs[row, column]
        x_values, y_values, average = civ.axes()
        yaverage = mean(y_values)
        civ_axs.plot(
            x_values,
            [0.5 - average for _ in y_values],
            color="lightgray",
            linestyle="dashed",
        )
        civ_axs.plot(x_values, [0 for _ in y_values], color="red", alpha=0.5)
        civ_axs.scatter(x_values, [y - yaverage for y in y_values], color="blue", s=7)
        civ_axs.plot(
            x_values, [y - yaverage for y in y_values], color="blue", linewidth=1
        )
        civ_axs.set_ylim([-0.10, 0.10])
        civ_axs.yaxis.set_visible(False)
        civ_axs.xaxis.set_visible(False)
        civ_axs.set_title(name)
    plt.subplots_adjust(wspace=0.25, hspace=0.45)
    if filename:
        fig.set_size_inches(19.2, 10.16)
        plt.savefig(filename)
    else:
        plt.show()


def plot_civ(civ, dirname="tmp"):
    """ Makes a plot for an individual civ."""
    name = civ_map()[civ.civ_id]
    fig = plt.figure()
    fig.set_size_inches(2, 1.5)
    x_values, y_values, average = civ.axes()
    yaverage = mean(y_values)
    plt.plot(
        x_values,
        [0.5 - average for _ in y_values],
        color="gray",
        linestyle=(0, (5, 7, 6, 7, 5, 7)),
    )
    plt.plot(x_values, [0 for _ in y_values], color="red", alpha=0.5)
    plt.scatter(x_values, [y - yaverage for y in y_values], color="blue", s=7)
    plt.plot(x_values, [y - yaverage for y in y_values], color="blue", linewidth=1)
    axes = plt.gca()
    axes.set_title(name, fontsize=10)
    axes.set_ylim([-0.10, 0.10])
    axes.yaxis.set_visible(False)
    axes.xaxis.set_visible(False)
    if dirname:

        plt.savefig("{}/{}.png".format(dirname, name.lower()), bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def run():
    """ Global variable hider."""
    dirname = "tmp"
    civs = loaded_civs()
    for civ in civs.values():
        if False:
            plot_civ(civ, dirname)

    plot_all_civs(civs)


if __name__ == "__main__":
    run()
