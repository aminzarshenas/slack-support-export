#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup

# -- PACKAGE VERSION -- #
current_version = "1.0.2"
#########################

documentation_packages = [
    'sphinx'
]
regular_packages = [
    'pandas',
    'slacker'
]

setup(
    name='supporttracker',
    python_requires='>=3.0.0',
    version=current_version,
    description='Tracker for Slack support requests',
    author='Amin Zarshenas',
    author_email='amin.zarshenas@gmail.com',
    packages=find_packages(),
    install_requires=[regular_packages],
    include_package_data=True,
    extras_require={
        'all': regular_packages + documentation_packages,
        'doc': documentation_packages
    }
)