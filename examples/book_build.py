'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


This example builds a book from the data in Arctic.
'''
import arctic
from sortedcontainers import SortedDict as sd


def main():
    a = arctic.Arctic('127.0.0.1')
    lib = a['BITMEX']
    it = lib.iterator('l2_book-XBTUSD')

    book = {'bid': sd(), 'ask': sd()}

    for chunk in it:
        for row in chunk.iterrows():
            timestamp = row[0]
            side, price, size, receipt_timestamp, delta = row[1].values
            if size == 0:
                del book[side][price]
            else:
                book[side][price] = size
            if delta:
                print(f"Time: {timestamp} L2 Book: {book}")
                best_bid, best_ask = book['bid'].keys()[-1], book['ask'].keys()[0]
                print("Best Bid:", best_bid)
                print("Best Ask:", best_ask)
                print("\n")


if __name__ == '__main__':
    main()