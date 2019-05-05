'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

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
    version="0.0.5",
    author="Bryant Moscon",
    author_email="bmoscon@gmail.com",
    description=("Storage engine for cryptocurrency data"),
    long_description=ld,
    long_description_content_type='text/x-rst',
    license="XFree86",
    keywords=["cryptocurrency", "bitcoin", "btc", "market data", "data storage", "redis", "database"],
    url="https://github.com/bmoscon/cryptostore",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=[
        "pandas",
        "cryptofeed>=0.22.0",
        "pyyaml",
        "redis",
        "pyarrow"
    ],
    extras_require={
        'arctic': ['arctic'],
        'gcs': ['google-cloud-storage'],
        'aws': ['boto3']
    },
    entry_points = {
        'console_scripts': ['cryptostore=cryptostore.bin.cryptostore:main'],
    }
)
