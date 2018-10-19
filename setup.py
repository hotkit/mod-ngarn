#!/usr/bin/env python
from distutils.core import setup

setup(
    name='mod_ngarn',
    version='0.1',
    description='Simple async worker',
    author='Proteus Tech',
    author_email='info@proteus-tech.com',
    url='https://proteus-tech.com/',
    scripts=[],
    packages=["mod_ngarn"],
    package_dir={'': 'Python'},
    install_requires=['asyncpg'],
    data_files=[
        ('share/mod-ngarn/Schema', ['Schema/001-initial.blue.sql']),
        ('share/mod-ngarn/Fostgres', ['Fostgres/mod-ngarn-job-view.json'])
    ]
)
