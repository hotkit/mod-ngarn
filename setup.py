#!/usr/bin/env python
from distutils.core import setup

setup(
    name='mod-ngarn',
    version='0.0.1',
    description='Simple async worker jobs',
    author='Proteus Tech',
    author_email='info@proteus-tech.com',
    url='https://proteus-tech.com/',
    scripts=[],
    packages=[],
    package_dir={'': 'Python'},
    install_requires=['asyncpg'],
    data_files=[
        ('share/mod-ngarn/Schema', ['Schema/001-initial.blue.sql'])
    ]
)
