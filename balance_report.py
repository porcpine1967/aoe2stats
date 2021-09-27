#!/usr/bin/env python
""" Analyze affects of civ balance changes. """
from collections import defaultdict
from datetime import datetime, timedelta

from report import parsed_args, ReportManager

CIV_REBALANCES = {
    51737: {
        "date": "20210810",
        "civs": [
            "BURMESE",
            "BYZANTINES",
            "CHINESE",
            "CUMANS",
            "LITHUANIANS",
            "MALIANS",
            "SICILIANS",
        ],
    },
    50292: {"date": "20210706", "civs": ["BURGUNDIANS", "PERSIANS", "SLAVS", "TURKS"]},
    47820: {
        "date": "20210503",
        "civs": [
            "BURGUNDIANS",
            "CUMANS",
            "FRANKS",
            "INCAS",
            "MALAY",
            "MONGOLS",
            "PORTUGUESE",
            "SICILIANS",
        ],
    },
    45185: {"date": "20210211", "civs": ["BURGUNDIANS", "SICILIANS"]},
    44725: {
        "date": "20210125",
        "civs": [
            "BULGARIANS",
            "BURMESE",
            "FRANKS",
            "HUNS",
            "ITALIANS",
            "KOREANS",
            "MAYANS",
            "SARACENS",
            "TATARS",
        ],
    },
    43210: {"date": "20201124", "civs": ["BURMESE"]},
    42848: {
        "date": "20201117",
        "civs": [
            "AZTECS",
            "BULGARIANS",
            "BURMESE",
            "CELTS",
            "CUMANS",
            "ETHIOPIANS",
            "GOTHS",
            "INDIANS",
            "ITALIANS",
            "KHMER",
            "KOREANS",
            "LITHUANIANS",
            "MALIANS",
            "MAYANS",
            "PORTUGUESE",
            "SPANISH",
            "TATARS",
            "TURKS",
            "VIKINGS",
        ],
    },
}


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


def run(version):
    """ Print results of version."""
    version_info = CIV_REBALANCES[version]
    print("Report on version", version)
    date = datetime.strptime(version_info["date"], "%Y%m%d")
    endtime = date + timedelta(days=8)
    reporter = ReportManager(parsed_args())
    reporter.generate(endtime)
    display(reporter, [civ.capitalize() for civ in version_info["civs"]])


if __name__ == "__main__":
    run(50292)
