#!/usr/bin/env python
from setuptools import setup, find_packages

# Package dependencies are handled externally via requirements.txt to please Docker and Travis

tests_require = ['nose', 'aioresponses', 'asynctest']

setup (
    name = "katsdpcontroller",
    description = "Service providing control and monitoring services for the MeerKAT Science Data Processor",
    author = "Simon Ratcliffe",
    packages = find_packages(),
    package_data={'katsdpcontroller': ['static/*', 'schemas/*', 'templates/*']},
    include_package_data = True,
    scripts = [
        "scripts/sdp_master_controller.py",
        "scripts/haproxy_disp.py"
        ],
    setup_requires = ['katversion'],
    install_requires = [
        'pymesos>=0.2.10',   # 0.2.10 implements suppressOffers
        'addict!=2.0.*',
        'aiohttp',
        'async_timeout',
        'decorator',
        'docker',
        'jsonschema',
        'rfc3987',           # Used by jsonschema to validate URLs
        'networkx>=2.0',
        'pydotplus',
        'netifaces',
        'aiokatcp>=0.2',
        'katsdptelstate',
        'katsdpservices',
        'kazoo',
        'prometheus_client'
    ],
    tests_require = tests_require,
    extras_require = {
        'agent': ['psutil', 'py3nvml', 'pycuda'],
        'haproxy_disp': ['jinja2'],
        'test': tests_require
    },
    use_katversion = True,
    license='MIT',
    zip_safe=False
)
