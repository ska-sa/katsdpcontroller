#!/usr/bin/env python
from setuptools import setup, find_packages

# Package dependencies are handled externally via requirements.txt to please Docker and Travis

tests_require = ['mock', 'unittest2', 'nose', 'futures', 'requests_mock']

setup (
    name = "katsdpcontroller",
    description = "Service providing control and monitoring services for the MeerKAT Science Data Processor",
    author = "Simon Ratcliffe",
    packages = find_packages(),
    package_data={'': ['conf/*']},
    include_package_data = True,
    scripts = [
        "scripts/sdp_master_controller.py",
        "scripts/haproxy_disp.py"
        ],
    setup_requires = ['katversion'],
    install_requires = [
        'pymesos>=0.2.9',
        'addict!=2.0.*',   # 2.0.* had quite a few bugs, particularly https://github.com/mewwts/addict/issues/83
        'trollius',
        'decorator',
        'docker',
        'jsonschema',
        'networkx',
        'netifaces',
        'enum34',
        'ipaddress',
        'requests',
        'six',
        'katcp',
        'tornado',
        'katsdptelstate',
        'katsdpservices',
        'faulthandler',
        'kazoo',
        'tornado>=4.3',
        'prometheus_client'
    ],
    tests_require = tests_require,
    extras_require = {
        'agent': ['psutil', 'nvidia-ml-py', 'pycuda'],
        'test': tests_require
    },
    use_katversion = True,
    license='MIT'
)
