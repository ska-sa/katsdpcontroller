#!/usr/bin/env python3
from setuptools import setup, find_packages

# Package dependencies are handled externally via requirements.txt to please Docker and Travis

tests_require = ['nose', 'aioresponses', 'asynctest', 'open-mock-file']

setup(
    name="katsdpcontroller",
    description="Service providing control and monitoring services for the "
                "MeerKAT Science Data Processor",
    author="Simon Ratcliffe",
    maintainer="Bruce Merry",
    packages=find_packages(),
    package_data={'katsdpcontroller': ['static/*', 'schemas/*', 'templates/*']},
    include_package_data=True,
    scripts=[
        "scripts/sdp_master_controller.py",
        "scripts/haproxy_disp.py"
        ],
    setup_requires=['katversion'],
    install_requires=[
        'pymesos>=0.3.6',   # 0.3.6 implements reviveOffers with roles
        'addict!=2.0.*',
        'aiohttp~=2.0',
        'aiomonitor',
        'async_timeout',
        'decorator',
        'docker',
        'jinja2',
        'jsonschema',
        'rfc3987',           # Used by jsonschema to validate URLs
        'networkx>=2.0',
        'pydotplus',
        'netifaces',
        'aiokatcp>=0.2',
        'katsdptelstate',
        'katsdpservices',
        'kazoo',
        'prometheus_client<0.4.0',   # 0.4.0 forces _total suffix
        'prometheus_async'
    ],
    tests_require=tests_require,
    extras_require={
        'agent': ['psutil', 'py3nvml', 'pycuda'],
        'test': tests_require
    },
    use_katversion=True,
    license='MIT',
    zip_safe=False
)
