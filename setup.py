from setuptools import find_packages

try:
    from core4.setup import setup
except:
    from core4.script.installer.core4.setup import setup

import home

setup(
    name="home",
    version=home.__version__,
    packages=find_packages(exclude=['docs*', 'tests*'])
)
