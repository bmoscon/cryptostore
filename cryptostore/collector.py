'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import inspect
import logging
import os
from multiprocessing import Process

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, CANDLES
from cryptofeed.exchanges import EXCHANGE_MAP

LOG = logging.getLogger('cryptostore')


class Collector(Process):
    def __init__(self, exchange, exchange_config, config):
        self.exchange = exchange
        self.exchange_config = exchange_config
        self.config = config
        super().__init__()
        self.daemon = True

    def _exchange_feed_options(self, feed_config: dict):
        """
        Parses feed config values from config.yaml.
        Exchange specific options will be derived from keyword args of the exchange FeedHandler class.

        ex: 'depth_interval' option will be consumed from config since it is found in cryptofeed.exchange.Binance.__init__
        """
        # base Feed options
        base_feed_options = {'max_depth', 'snapshot_interval'}

        # exchange specific Feed options
        ExchangeFeedClass = EXCHANGE_MAP[self.exchange]
        exchange_feed_options = inspect.signature(ExchangeFeedClass.__init__).parameters
        exchange_feed_options = {key for key in exchange_feed_options if key not in {'self', '**kwargs'}}

        available_options = base_feed_options.union(exchange_feed_options)

        fh_kwargs = {}
        for key, value in feed_config.items():
            if key in available_options:
                fh_kwargs[key] = value

        return fh_kwargs

    def run(self):
        LOG.info("Collector for %s running on PID %d", self.exchange, os.getpid())
        cache = self.config['cache']
        retries = self.exchange_config.pop('retries', 30)
        timeouts = self.exchange_config.pop('channel_timeouts', {})
        http_proxy = self.exchange_config.pop('http_proxy', None)
        fh = FeedHandler()

        for callback_type, feed_config in self.exchange_config.items():
            # config value can be a dict or list of symbols
            feed_kwargs = {'retries': retries, 'timeout': timeouts.get(callback_type, 120)}
            if http_proxy is not None:
                feed_kwargs['http_proxy'] = http_proxy

            if isinstance(feed_config, dict):
                feed_kwargs.update(self._exchange_feed_options(feed_config))

            cb = {}
            if callback_type in (L2_BOOK, L3_BOOK):
                self.exchange_config[callback_type] = self.exchange_config[callback_type]['symbols']

            if cache == 'redis':
                if 'ip' in self.config['redis'] and self.config['redis']['ip']:
                    kwargs = {'host': self.config['redis']['ip'], 'port': self.config['redis']['port'], 'numeric_type': float}
                else:
                    kwargs = {'socket': self.config['redis']['socket'], 'numeric_type': float}
                from cryptofeed.backends.redis import TradeStream, BookStream, TickerStream, FundingStream, OpenInterestStream, LiquidationsStream, CandlesStream
                trade_cb = TradeStream
                book_cb = BookStream
                ticker_cb = TickerStream
                funding_cb = FundingStream
                oi_cb = OpenInterestStream
                liq_cb = LiquidationsStream
                candles_cb = CandlesStream
            elif cache == 'kafka':
                from cryptofeed.backends.kafka import TradeKafka, BookKafka, TickerKafka, FundingKafka, OpenInterestKafka, LiquidationsKafka, CandlesKafka
                trade_cb = TradeKafka
                book_cb = BookKafka
                ticker_cb = TickerKafka
                funding_cb = FundingKafka
                oi_cb = OpenInterestKafka
                liq_cb = LiquidationsKafka
                candles_cb = CandlesKafka
                kwargs = {'bootstrap': self.config['kafka']['ip'], 'port': self.config['kafka']['port']}

            if callback_type == TRADES:
                cb[TRADES] = [trade_cb(**kwargs)]
            elif callback_type == LIQUIDATIONS:
                cb[LIQUIDATIONS] = [liq_cb(**kwargs)]
            elif callback_type == FUNDING:
                cb[FUNDING] = [funding_cb(**kwargs)]
            elif callback_type == TICKER:
                cb[TICKER] = [ticker_cb(**kwargs)]
            elif callback_type == L2_BOOK:
                cb[L2_BOOK] = [book_cb(key=L2_BOOK, snapshot_interval=feed_config.get('snapshot_interval', 1000), **kwargs)]
            elif callback_type == L3_BOOK:
                cb[L3_BOOK] = [book_cb(key=L3_BOOK, snapshot_interval=feed_config.get('snapshot_interval', 1000), **kwargs)]
            elif callback_type == OPEN_INTEREST:
                cb[OPEN_INTEREST] = [oi_cb(**kwargs)]
            elif callback_type == CANDLES:
                cb[CANDLES] = [candles_cb(**kwargs)]

            if 'pass_through' in self.config:
                if self.config['pass_through']['type'] == 'zmq':
                    from cryptofeed.backends.zmq import TradeZMQ, BookZMQ, FundingZMQ, OpenInterestZMQ, TickerZMQ, LiquidationsZMQ, CandlesZMQ
                    host = self.config['pass_through']['host']
                    port = self.config['pass_through']['port']

                    if callback_type == TRADES:
                        cb[TRADES].append(TradeZMQ(host=host, port=port))
                    elif callback_type == LIQUIDATIONS:
                        cb[LIQUIDATIONS].append(LiquidationsZMQ(host=host, port=port))
                    elif callback_type == FUNDING:
                        cb[FUNDING].append(FundingZMQ(host=host, port=port))
                    elif callback_type == L2_BOOK:
                        cb[L2_BOOK].append(BookZMQ(host=host, port=port, snapshot_interval=feed_config.get('snapshot_interval', 1000)))
                    elif callback_type == L3_BOOK:
                        cb[L3_BOOK].append(BookZMQ(host=host, port=port, snapshot_interval=feed_config.get('snapshot_interval', 1000)))
                    elif callback_type == OPEN_INTEREST:
                        cb[OPEN_INTEREST].append(OpenInterestZMQ(host=host, port=port))
                    elif callback_type == TICKER:
                        cb[TICKER].append(TickerZMQ(host=host, port=port))
                    elif callback_type == CANDLES:
                        cb[CANDLES].append(CandlesZMQ(host=host, port=port))

            fh.add_feed(self.exchange, subscription={callback_type: self.exchange_config[callback_type]}, callbacks=cb, **feed_kwargs)
            LOG.info(f"Collector added feed handler - {self.exchange}({callback_type.upper()}, {feed_kwargs})")

        fh.run()
