FROM python:3.7.3-stretch

COPY config_final.yaml /config.yaml
COPY setup.py /
COPY cryptostore /cryptostore

RUN apt install gcc git

RUN pip install --no-cache-dir git+https://github.com/bmoscon/cryptofeed.git
RUN pip install --no-cache-dir cython
RUN pip install --no-cache-dir pyarrow
RUN pip install --no-cache-dir redis
RUN pip install --no-cache-dir aioredis
RUN pip install --no-cache-dir arctic
RUN pip install --no-cache-dir boto3

RUN pip install -e .

ENTRYPOINT [ "cryptostore" ]
