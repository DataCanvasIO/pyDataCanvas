#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup

ROOT_DIR = os.path.dirname(__file__)
SOURCE_DIR = os.path.join(ROOT_DIR)

with open('./requirements.txt') as reqs_txt:
    requirements = [line for line in reqs_txt]

exec(open('datacanvas/version.py').read())

with open('./test-requirements.txt') as test_reqs_txt:
    test_requirements = [line for line in test_reqs_txt]

setup(
    name="pyDataCanvas",
    version=version,
    description="Runtime Support for DataCanvas.",
    packages=['datacanvas'],
    author="Xiaolin Zhang",
    author_email="leoncamel@gmail.com",
    install_requires=requirements,
    tests_require=test_requirements,
    zip_safe=False,
    test_suite='tests',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
    ],
)
