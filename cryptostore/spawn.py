from multiprocessing import Process
import logging

from cryptostore.collector import Collector


LOG = logging.getLogger('cryptostore')


class Spawn(Process):
    def __init__(self, queue):
        super()
        self.queue = queue
    
    def run(self):
        procs = {}
        try:
            while True:
                message = self.queue.get()
                operation, exchange = message.split("*")
                if operation == 'kill':
                    LOG.info("Terminating %s", exchange)
                    procs[exchange].terminate()
                    del procs[exchange]
                elif operation == 'start':
                    LOG.info("Starting %s", message)
                    # spawn a cryptofeed handler
                    if message in procs:
                        LOG.warning("Collector exists for %s", message)
                        continue
                    procs[exchange] = Collector(exchange)
                    procs[exchange].start()
        except KeyboardInterrupt:
            pass