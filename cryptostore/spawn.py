"""
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
"""
from multiprocessing import Process
import logging
import json
import os

from cryptostore.collector import Collector


LOG = logging.getLogger("cryptostore")


class Spawn(Process):
    """
    This class spawns/stops cryptofeed handlers.
    These handlers are then properly defined in the Collector class, and the
    options defined in the config_file and turned into a FeedHandler object
    which connects directly to an exchange data feed. 
    """

    def __init__(self, queue):
        self.queue = queue
        super().__init__()

    def run(self):
        LOG.info("Spawner running on PID %d", os.getpid())
        procs = {}
        try:
            while True:
                message = self.queue.get()
                LOG.info("message: %s", str(message))
                msg = json.loads(message)
                if msg["op"] == "stop":
                    exchange = msg["exchange"]
                    LOG.info("Terminating %s", exchange)
                    procs[exchange].terminate()
                    del procs[exchange]
                elif msg["op"] == "start":
                    LOG.info("Starting %s", msg)
                    exchange = msg["exchange"]
                    collector = msg["collector"]
                    config = msg["config"]
                    # spawn a cryptofeed handler
                    if exchange in procs:
                        LOG.warning("Collector exists for %s", exchange)
                        continue
                    procs[exchange] = Collector(exchange, collector, config)
                    procs[exchange].start()
        except KeyboardInterrupt:
            pass
