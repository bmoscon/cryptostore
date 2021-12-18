'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from multiprocessing import Process
import logging
import json
import os

import signal

from cryptostore.collector import Collector
from cryptostore.util import setup_process_signal_handlers


LOG = logging.getLogger('cryptostore')


class Spawn(Process):
    def __init__(self, queue):
        self.queue = queue
        self.procs = {}
        self.terminating = False
        super().__init__()

    def _stop_on_signal(self, signal, *args):
        if self.terminating:
            LOG.info('Spawner is already being stopped...')
            return

        LOG.info("Stopping Spawner due to signal %d", signal)
        self.terminating = True
        self.queue.close()

        # send termination signals to children
        for proc in self.procs.values():
            proc.terminate()

        # wait for termination to complete
        for proc in self.procs.values():
            if proc.is_alive():
                proc.join()

    def run(self):
        LOG.info("Spawner running on PID %d", os.getpid())
        setup_process_signal_handlers(self._stop_on_signal)
        try:
            while not self.terminating:
                try:
                    message = self.queue.get()
                except (ValueError, OSError) as e:
                    LOG.info("Config queue get interrupt")
                    # Queue has been marked as being closed by this/another process
                    # Skip processing of enqued configs
                    continue
                LOG.info("message: %s", str(message))
                msg = json.loads(message)
                if msg['op'] == 'stop':
                    exchange = msg['exchange']
                    LOG.info("Terminating %s", exchange)
                    self.procs[exchange].terminate()
                    del self.procs[exchange]
                elif msg['op'] == 'start':
                    LOG.info("Starting %s", msg)
                    exchange = msg['exchange']
                    collector = msg['collector']
                    config = msg['config']
                    # spawn a cryptofeed handler
                    if exchange in self.procs:
                        LOG.warning("Collector exists for %s", exchange)
                        continue
                    self.procs[exchange] = Collector(exchange, collector, config)
                    self.procs[exchange].start()
        except KeyboardInterrupt:
            pass

        LOG.info("Spawner stopped")
