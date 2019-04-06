from multiprocessing import Process


class Collector(Process):
    def __init__(self, exchange, config):
        self.exchange = exchange
        self.config = config
        super().__init__()

    
    def run(self):
        pass
    
    