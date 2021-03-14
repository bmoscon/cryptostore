'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from datetime import timedelta, timezone, datetime as dt


def get_time_interval(ts: float, interval: str, multiplier=1):
    timestamp = dt.utcfromtimestamp(ts)
    start_interval, end_interval = None, None
    if interval == 'M':
        end_interval = dt(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute)
        start_interval = end_interval - timedelta(minutes=multiplier)
    elif interval == 'H':
        end_interval = dt(timestamp.year, timestamp.month, timestamp.day, timestamp.hour)
        start_interval = end_interval - timedelta(hours=multiplier)
    elif interval == 'D':
        end_interval = dt(timestamp.year, timestamp.month, timestamp.day)
        start_interval = end_interval - timedelta(days=multiplier)
    if start_interval and end_interval:
        start_interval = int(start_interval.replace(tzinfo=timezone.utc).timestamp()) * 1000
        end_interval = int(end_interval.replace(tzinfo=timezone.utc).timestamp()) * 1000
    return start_interval, end_interval
