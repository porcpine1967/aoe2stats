#!/usr/bin/env python
""" Analyze affects of civ balance changes. """
from collections import defaultdict
from datetime import datetime, timedelta

from report import arg_parser, ReportManager

from utils.versions import VERSIONS as CIV_REBALANCES


def display(reporter, civ_names):
    """ Display information on civs in civ names."""
    args = reporter.args
    categories = reporter.categories
    for report_type in reporter.report_types:
        print(report_type.capitalize())
        data = defaultdict(list)
        template = "{:^" + str(18 + 12 * args.s) + "}"
        print(
            "    ".join([template for _ in range(len(categories))]).format(*categories)
        )
        for category in categories:
            for civ in reporter.civs.values():
                if civ.name in civ_names:
                    data[category].append(civ)
        for i in range(len(civ_names)):
            print(
                "    ".join(["{}" for _ in range(len(categories))]).format(
                    *[
                        data[categories[j]][i].info(report_type, categories[j], args.s)
                        for j in range(len(categories))
                    ]
                )
            )


def run():
    """ Print results of version."""
    parser = arg_parser()
    parser.add_argument("version", type=int, help="Which version to use")
    args = parser.parse_args()
    version_info = CIV_REBALANCES[args.version]
    print("Report on version", args.version)
    date = datetime.strptime(version_info["date"], "%Y%m%d")
    endtime = date + timedelta(days=8)
    reporter = ReportManager(args)
    reporter.generate(endtime)
    display(reporter, [civ.capitalize() for civ in version_info["civs"]])


if __name__ == "__main__":
    run()
