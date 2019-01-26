from setuptools import setup
from setuptools import find_packages


setup(
    name="cryptostore",
    version="0.1",
    author="Bryant Moscon",
    author_email="bmoscon@gmail.com",
    packages=find_packages(),
    install_requires=[
        "cryptofeed",
        "pyyaml"
    ],
)