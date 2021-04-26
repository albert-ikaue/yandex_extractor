"""A setuptools based setup module."""
from setuptools import setup

from setup_common import define_args

args = define_args()

setup(**args)
