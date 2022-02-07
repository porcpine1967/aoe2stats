#!/usr/bin/env python
""" Information on different versions of DE."""

from datetime import datetime, timezone

VERSIONS = {
    54684: {"date": "20211007", "civs": []},
    54480: {
        "date": "20211005",
        "civs": ["BULGARIANS", "BURGUNDIANS", "PORTUGUESE", "SICILIANS", "VIETNAMESE",],
    },
    44834: {"date": "20210128", "civs": []},
    45340: {"date": "20210212", "civs": []},
    50700: {"date": "20210712", "civs": []},
    53347: {"date": "20210908", "civs": []},
    46295: {"date": "20210324", "civs": []},
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


def version_for_date(date):
    """ Returns version earliest before date"""
    best_version = None
    best_date = None
    for version, values in VERSIONS.items():
        version_date = values["date"]
        if int(version_date) < int(date):
            if not best_date:
                best_date = version_date
                best_version = version
            else:
                if int(best_date) < int(version_date):
                    best_date = version_date
                    best_version = version
    return str(best_version) if best_version else None


def version_for_timestamp(timestamp):
    """ Returns version earliest before timestamp"""
    return version_for_date(
        datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y%m%d")
    )
