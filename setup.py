#!/usr/bin/env python
from setuptools import setup, find_packages

# Package dependencies are handled externally via requirements.txt to please Docker and Travis

setup (
    name = "katsdpcontroller",
    version = "trunk",
    description = "Service providing control and monitoring services for the MeerKAT Science Data Processor",
    author = "Simon Ratcliffe",
    packages = find_packages(),
    package_data={'': ['conf/*']},
    include_package_data = True,
    scripts = [
        "scripts/sdp_master_controller.py",
        ],
    license='MIT',
    zip_safe = False,
)
