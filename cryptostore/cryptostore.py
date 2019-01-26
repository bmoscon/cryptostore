from multiprocessing import Process, Queue

from cryptostore.spawn import Spawn
from cryptostore.log import get_logger


LOG = get_logger('cryptostore', 'cryptostore.log')


class Cryptostore:
    def __init__(self):
        self.queue = Queue()
        self.spawner = Spawn(self.queue)
    
    def run(self):
        self.spawner.start()
