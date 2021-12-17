'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''

import asyncio
import functools
import signal

from datetime import timedelta, timezone, datetime as dt
from signal import SIGABRT, SIGINT, SIGTERM


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

def get_stop_signals():
    try:
        # unix / macos only
        from signal import SIGHUP
        signals = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
    except ImportError:
        signals = (SIGABRT, SIGINT, SIGTERM)

    return signals

def stop_event_loop(loop, callback=None):

    async def _gather_tasks_and_stop():
        pending = [task for task in asyncio.all_tasks(loop=loop) if task is not
                 asyncio.current_task(loop=loop)]
        for task in pending:
            task.cancel()
        if callback:
            pending.append(loop.create_task(callback))
        await asyncio.gather(*pending, loop=loop, return_exceptions=True)
        loop.stop()

    loop.create_task(_gather_tasks_and_stop())

def setup_event_loop_signal_handlers(loop, callback, *args):
    for sig in get_stop_signals():
        loop.add_signal_handler(sig, functools.partial(callback, sig=sig, loop=loop, *args))

def setup_process_signal_handlers(callback):
    for sig in get_stop_signals():
        signal.signal(sig, callback)