'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os

from cryptostore.config import Config


class PluginController:
    def __init__(self, config: str):
        super().__init__()
        self.plugins = []
        self.cfg = None
        if config:
            self.cfg = Config(config)
        else:
            if 'CRYPTOSTORE_CONFIG' in os.environ:
                file_name = os.environ['CRYPTOSTORE_CONFIG']
            else:
                file_name = os.path.join(os.getcwd(), 'config.yaml')

            if os.path.isfile(file_name):
                self.cfg = Config(file_name)

    def start(self):
        if 'plugins' in self.cfg:
            if self.cfg and isinstance(self.cfg.plugins, dict):
                for _, plugin in self.cfg.plugins.items():
                    module = plugin.module
                    if isinstance(module, list):
                        obj = getattr(__import__(module[0], fromlist=[module[1]]), module[1])
                    else:
                        obj = __import__(module)
                    self.plugins.append(obj(plugin.config))
                    self.plugins[-1].start()

    def stop(self):
        for p in self.plugins:
            p.terminate()
