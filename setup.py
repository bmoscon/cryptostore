'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import sys

from setuptools import setup
from setuptools import find_packages
from setuptools.command.test import test as TestCommand

ld = None
try:
    import pypandoc
    ld = pypandoc.convert_file('README.md', 'rst', format='markdown_github')
except BaseException:
    pass


setup(
    name="cryptostore",
    version="0.0.2",
    author="Bryant Moscon",
    author_email="bmoscon@gmail.com",
    description=("Storage engine for cryptocurrency data"),
    long_description=ld,
    long_description_content_type='text/x-rst',
    license="XFree86",
    keywords=["cryptocurrency", "bitcoin", "btc", "feed handler", "market feed", "market data", "data storage"],
    url="https://github.com/bmoscon/cryptostore",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=[
        "pandas",
        "cryptofeed",
        "pyyaml",
        "aioredis",
        "pyarrow",
        "arctic",
        "google-cloud-storage",
        "boto3"
    ],
)
