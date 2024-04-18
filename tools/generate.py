'''
Copyright (C) 2018-2024  Bryant Moscon - bmoscon@gmail.com
Please see the LICENSE file for the terms and conditions
associated with this software.


Requires a config file named "config.yaml" in the same directory,
see provided example.
'''
import yaml

from cryptofeed.exchanges import EXCHANGE_MAP


def main():
    with open("config.yaml", 'r') as fp:
        config = yaml.safe_load(fp)

    kwargs = config.copy()
    [kwargs.pop(key) for key in ('backend', 'channels', 'exchanges', 'symbols', 'symbols_per_channel')]
    kwargs_str = '-e ' + ' -e '.join([f"{key.upper()}='{value}'" for key, value in kwargs.items()])

    for exchange in config['exchanges']:
        eobj = EXCHANGE_MAP[exchange.upper()]()
        symbols = eobj.symbols()

        if config['symbols'][exchange] != 'ALL':
            if any([s not in symbols for s in config['symbols'][exchange]]):
                print("Invalid symbol specified")
                return
            symbols = config['symbols'][exchange]
        
        for i in range(0, len(symbols), config['symbols_per_channel']):
            for chan in config['channels']:
                print(f"docker run -e EXCHANGE={exchange.upper()} -e CHANNELS={chan} -e SYMBOLS='{','.join(symbols[i:i+config['symbols_per_channel']])}' -e BACKEND={config['backend']} {kwargs_str} ghcr.io/bmoscon/cryptostore:latest")


if __name__ == '__main__':
    main()
