from multiprocessing import Process
import logging
import json

from cryptostore.collector import Collector


LOG = logging.getLogger('cryptostore')


class Spawn(Process):
    def __init__(self, queue):
        self.queue = queue
        super().__init__()

    def run(self):
        procs = {}
        try:
            while True:
                message = self.queue.get()
                LOG.info("message: %s", str(message))
                msg = json.loads(message)
                if msg['op'] == 'stop':
                    exchange = msg['exchange']
                    LOG.info("Terminating %s", exchange)
                    procs[exchange].terminate()
                    del procs[exchange]
                elif msg['op'] == 'start':
                    LOG.info("Starting %s", msg)
                    exchange = msg['exchange']
                    config = msg['data']
                    # spawn a cryptofeed handler
                    if exchange in procs:
                        LOG.warning("Collector exists for %s", exchange)
                        continue
                    procs[exchange] = Collector(exchange, config)
                    procs[exchange].start()
        except KeyboardInterrupt:
            pass