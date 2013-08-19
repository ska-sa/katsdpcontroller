#!/usr/bin/env python
from setuptools import setup, find_packages

tests_require = [
]

install_requires = [
    'scikits.fitting',
    'katcp',
]

setup (
    name = "katsdpproxy",
    version = "trunk",
    description = "Proxy providing control and monitoring services for the MeerKAT Science Data Processor",
    author = "Simon Ratcliffe",
    packages = find_packages(),
    package_data={'': ['conf/*']},
    include_package_data = True,
    scripts = [
        "scripts/sdp_proxy.py",
        ],
    #install_requires = install_requires,
    tests_require=tests_require,
    license='MIT',
    zip_safe = False,
)
