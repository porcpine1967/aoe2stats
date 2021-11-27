#!/usr/bin/env python
""" Writes out map popularity of last two pools."""
from datetime import datetime, timedelta

from utils.map_pools import map_type_filter, pools
from utils.tools import execute_sql, last_time_breakpoint, map_name_lookup

SQL = """SELECT map_type, COUNT(*) as cnt
FROM matches
WHERE started BETWEEN {:0.0f} AND {:0.0f}
{}
AND team_size = {}
GROUP BY map_type
ORDER BY cnt DESC"""


def run():
    """ Run the report."""
    map_names = map_name_lookup()
    weeks = pools()[-2:]
    for size in (1, 2):
        print("TEAM" if size > 1 else "1v1")
        week_infos = []
        for idx, week in enumerate(weeks):
            week_info = []
            year = int(week[:4])
            month = int(week[4:6])
            day = int(week[6:])
            start = last_time_breakpoint(datetime(year, month, day))
            end = start + timedelta(days=14)
            sql = SQL.format(
                start.timestamp(), end.timestamp(), map_type_filter(week, size), size
            )
            total = 0
            for map_type, count in execute_sql(sql):
                week_info.append((map_names[map_type], count,))
                total += count
            hold = []
            for name, count in week_info:
                hold.append("{:17}: {:4.1f}%".format(name, 100.0 * count / total))
            week_infos.append(hold)
        print("{:^24}     {:^24}".format(*weeks))
        for idx in range(len(week_infos[0])):
            print("{}      {}".format(week_infos[0][idx], week_infos[1][idx]))


if __name__ == "__main__":
    run()
