'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from setuptools import setup
from setuptools import find_packages


ld = None
try:
    import pypandoc
    ld = pypandoc.convert_file('README.md', 'rst', format='markdown_github')
except BaseException:
    pass


setup(
    name="cryptostore",
    version="0.2.0",
    author="Bryant Moscon",
    author_email="bmoscon@gmail.com",
    description=("Storage engine for cryptocurrency data"),
    long_description=ld,
    long_description_content_type='text/x-rst',
    license="XFree86",
    keywords=["cryptocurrency", "bitcoin", "btc", "market data", "data storage", "redis", "database", "kafka"],
    url="https://github.com/bmoscon/cryptostore",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=[
        "pandas",
        "cryptofeed>=1.0.0",
        "pyyaml",
        "pyarrow",
        "aiohttp"
    ],
    extras_require={
        'redis': ['redis', 'aioredis'],
        'kafka': ['aiokafka', 'confluent-kafka'],
        'arctic': ['arctic'],
        'gcs': ['google-cloud-storage'],
        'aws': ['boto3'],
        'zmq': ['pyzmq']
    },
    entry_points = {
        'console_scripts': ['cryptostore=cryptostore.bin.cryptostore:main'],
    }
)
