import argparse

from cryptostore import Cryptostore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help='path to the config file')
    args = parser.parse_args()

    cs = Cryptostore(config=args.config)
    try:
        cs.run()
    except KeyboardInterrupt:
        pass
