#!/usr/bin/env python3
from setuptools import setup, find_packages

tests_require = ['nose', 'aioresponses>=0.6.4', 'asynctest', 'open-mock-file']

setup(
    name="katsdpcontroller",
    description="Service providing control and monitoring services for the "
                "MeerKAT Science Data Processor",
    author="MeerKAT SDP Team",
    author_email="sdpdev+katsdpcontroller@ska.ac.za",
    packages=find_packages(),
    package_data={'katsdpcontroller': ['static/*', 'schemas/*', 'templates/*', 'assets/*']},
    include_package_data=True,
    scripts=[
        "scripts/sdp_master_controller.py",
        "scripts/sdp_product_controller.py"
        ],
    setup_requires=['katversion'],
    install_requires=[
        'pymesos>=0.3.6',    # 0.3.6 implements reviveOffers with roles
        'addict!=2.0.*,!=2.4.0',
        'aiohttp~=3.5',
        'aiohttp-jinja2',
        'aiomonitor',
        'aioredis',
        'async_timeout',
        'decorator',
        'docker',
        'jinja2',
        'jsonschema>=3.0',   # Version 3 implements Draft 7
        'rfc3987',           # Used by jsonschema to validate URLs
        'networkx>=2.0',
        'pydotplus',
        'netifaces',
        'aiokatcp>=0.6.1',
        'katdal',
        'katsdpmodels[aiohttp]',
        'katsdptelstate[aio]',
        'katsdpservices',
        'katportalclient',
        'kazoo',
        'aiozk',
        'yarl',
        'prometheus_client<0.4.0',   # 0.4.0 forces _total suffix
        'prometheus_async',
        'dash',
        'dash-core-components',
        'dash-html-components',
        'dash-table',
        'dash-dangerously-set-inner-html'
    ],
    tests_require=tests_require,
    extras_require={
        'agent': ['psutil', 'py3nvml', 'pycuda'],
        'test': tests_require
    },
    python_requires='>=3.6',
    use_katversion=True,
    license='MIT',
    zip_safe=False
)
