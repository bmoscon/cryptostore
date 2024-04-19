# Cryptostore

[![License](https://img.shields.io/badge/license-XFree86-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.8+-green.svg)
[![Docker Image](https://img.shields.io/badge/Docker-cryptostore-brightgreen.svg)](https://github.com/bmoscon/cryptostore/pkgs/container/cryptostore)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/da2a982c976649e193c807895ee7a33c)](https://www.codacy.com/manual/bmoscon/cryptostore?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=bmoscon/cryptostore&amp;utm_campaign=Badge_Grade)

Cryptostore is an application wrapper around [Cryptofeed](https://github.com/bmoscon/cryptofeed) that runs in containers, for the purpose of storing cryptocurrency data directly to various databases and message protocols.

This project assumes familiarity with Docker, but some basic commands are available to get you started below.

### Using a Prebuilt Container Image

Docker images are hosted in GitHub and can be pulled using the following command:

```
docker pull ghcr.io/bmoscon/cryptostore:latest
```

### To Build a Container From Source

```
docker build . -t "cryptostore:latest"
```


### To Run a Container

```
docker run -e EXCHANGE='COINBASE' -e CHANNELS='trades' -e SYMBOLS='BTC-USD' -e BACKEND='REDIS' cryptostore:latest
```

**Note**: if you've pulled the image from GitHub, the container name will be `ghcr.io/bmoscon/cryptostore:latest` as opposed to `cryptostore:latest`.


Depending on your operating system and how your backends are set up, networking configuration may need to be supplied to docker, or other backend specific environment variables might need to be supplied. 

Configuration is passed to the container via environment variables. `CHANNELS` and `SYMBOLS` can be single values, or list of values (separated by comas). Only one exchange per container is supported. For candlestick data, only one candle interval per feed is supported.


### Documentation

For more information about usage, see the [documentation](docs/).
