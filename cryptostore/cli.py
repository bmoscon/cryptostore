'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import argparse
import asyncio
import logging
import sys
from difflib import get_close_matches

from cryptofeed.exchanges import EXCHANGE_MAP

from cryptostore import __version__
from cryptostore.backends import BackendError, describe
from cryptostore.config import CONFIG_ENV, DEFAULT_CONFIG_PATH, ConfigError, config_path, feed_options, is_pattern, load, resolve_exchange
from cryptostore.runner import Cryptostore, check_health, configure_logging


LOG = logging.getLogger('cryptostore')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='cryptostore', description='Collect cryptocurrency market data with cryptofeed, store it and stream it')
    parser.add_argument('--version', action='version', version=f'cryptostore {__version__}')
    sub = parser.add_subparsers(dest='command')

    def with_config(p):
        p.add_argument('config', nargs='?', help=f'config file (default: ${CONFIG_ENV} or ./{DEFAULT_CONFIG_PATH})')

    run = sub.add_parser('run', help='run the collector')
    with_config(run)

    check = sub.add_parser('check', help='validate a config and show the worker plan, without connecting to any backend or exchange feed '
                                         '(symbol patterns such as all or *-USDT are expanded from the exchange\'s symbol list)')
    with_config(check)
    check.add_argument('--symbols', action='store_true', help='also download each exchange\'s symbol list and verify every explicitly configured symbol exists')

    health = sub.add_parser('health', help='exit 0 if the running collector wrote a fresh health file (for container health checks)')
    with_config(health)
    health.add_argument('--max-age', type=float, default=None, help='maximum age of the health file in seconds (default: 3x stats_interval)')

    exchanges = sub.add_parser('exchanges', help='list supported exchanges and their channels')
    exchanges.add_argument('exchange', nargs='?', help='show one exchange in detail')

    sub.add_parser('backends', help='list backend types')
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or 'run'

    try:
        if command == 'run':
            return cmd_run(args)
        if command == 'check':
            return cmd_check(args)
        if command == 'health':
            return cmd_health(args)
        if command == 'exchanges':
            return cmd_exchanges(args)
        if command == 'backends':
            print(describe())
            return 0
    except (ConfigError, BackendError) as e:
        print(f'error: {e}', file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    parser.print_help()
    return 2


def cmd_run(args) -> int:
    config = load(getattr(args, 'config', None))
    configure_logging(config.log_level, config.log_file)
    app = Cryptostore(config)
    app.build()
    return app.run()


def cmd_check(args) -> int:
    path = config_path(args.config)
    config = load(path)
    app = Cryptostore(config)
    app.build(record=False)
    print(f'{path}: ok')
    print(app.describe())

    if args.symbols:
        bad = asyncio.run(_check_symbols(config))
        if bad:
            for line in bad:
                print(f'error: {line}', file=sys.stderr)
            return 1
        print('symbols: ok')
    return 0


async def _check_symbols(config) -> list:
    problems = []
    for exchange in config.exchanges:
        cls = EXCHANGE_MAP[exchange.name]
        try:
            await cls.load_symbols()
        except Exception as e:
            problems.append(f'{exchange.name}: unable to load symbols ({type(e).__name__}: {e})')
            continue
        known = cls.symbols()
        known_set = set(known)
        for channel in exchange.channels:
            for symbol in channel.symbols:
                if is_pattern(symbol):
                    continue   # expanded from this very list, so always valid
                if symbol not in known_set:
                    close = get_close_matches(symbol, known, n=3)
                    hint = f" - did you mean {', '.join(close)}?" if close else ''
                    problems.append(f'{exchange.name} {channel.channel}: {symbol} is not a known symbol{hint}')
    return problems


def cmd_health(args) -> int:
    config = load(getattr(args, 'config', None))
    if not config.health_file:
        print('health_file is not configured - nothing to check')
        return 0
    if config.stats_interval <= 0:
        print('stats_interval is 0, so the health file is never refreshed - nothing to check')
        return 0
    max_age = args.max_age if args.max_age is not None else 3 * config.stats_interval
    healthy, message = check_health(config.health_file, max_age)
    print(message)
    return 0 if healthy else 1


def cmd_exchanges(args) -> int:
    if args.exchange:
        name = resolve_exchange(args.exchange)
        cls = EXCHANGE_MAP[name]
        print(name)
        print(f'  channels: {", ".join(cls.websocket_channels)}')
        if cls.valid_candle_intervals is not NotImplemented:
            print(f'  candle intervals: {", ".join(sorted(cls.valid_candle_intervals))}')
        print(f'  options: {", ".join(sorted(feed_options(cls)))}')
        return 0

    width = max(len(name) for name in EXCHANGE_MAP)
    for name, cls in sorted(EXCHANGE_MAP.items()):
        print(f'{name:<{width}}  {", ".join(cls.websocket_channels)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
