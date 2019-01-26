from multiprocessing import Process, Queue
import asyncio
import logging

from cryptostore.spawn import Spawn
from cryptostore.config import Config
from cryptostore.log import get_logger


LOG = get_logger('cryptostore', 'cryptostore.log', logging.INFO)


class Cryptostore:
    def __init__(self):
        self.queue = Queue()
        self.spawner = Spawn(self.queue)
        self.running_config = {}
    
    async def _reconfigure(self, config):
        if self.running_config != config:
            pass

    def run(self):
        LOG.info("Starting cryptostore")
        self.spawner.start()
        LOG.info("Spawner started")
        
        loop = asyncio.get_event_loop()
        self.config = Config(callback=self._reconfigure)
        LOG.info("Cryptostore started")
        loop.run_forever()
