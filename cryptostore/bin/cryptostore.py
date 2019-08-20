import argparse

from cryptostore import Cryptostore


def main():
    """
    The main() function of cryptostore, where the program starts.

    Takes one argument, a config file. 
    Cryptostore comes with a default config_file which can be modified to suit
    ones needs. 
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to the config file.")
    args = parser.parse_args()

    cs = Cryptostore(config=args.config)
    try:
        cs.run()
    except KeyboardInterrupt:
        pass
