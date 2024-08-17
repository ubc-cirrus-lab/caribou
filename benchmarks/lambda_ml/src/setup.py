#!/usr/bin/env python

import os
from setuptools import setup, find_packages

version = "0.0.1"

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

REQUIREMENTS = [
    'pytorch>=1.0.1'
]

setup(
    name='LambdaML',
    version=version,
    description='Machine learnong on serverless platform',
    long_description=README,
    author='DS3Lab',
    author_email='jiawei.jiang@inf.ethz.ch',
    url='https://github.com/DS3Lab/LambdaML',
    license="Apache",
    install_requires=REQUIREMENTS,
    keywords=['demo', 'setup.py', 'project'],
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
    ],
    entry_points={
        'console_scripts': ['demo = demo.demo_handler:main']
    },
)