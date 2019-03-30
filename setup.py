from core4.setup import setup
from setuptools import find_packages

import home

setup(
    name="home",
    version=home.__version__,
    packages=find_packages(exclude=['docs*', 'tests*'])
)
