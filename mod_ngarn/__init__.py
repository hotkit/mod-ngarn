"""Simple async worker"""

__version__ = "2.4"

import asyncio
import os

import click

from . import utils
from .worker import JobRunner

global script
global run
global init_table


@click.group()
def script():
    pass


@click.command()
def run():
    job_runner = JobRunner()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(job_runner.run())


@click.command()
@click.option(
    '--name',
    help='mod-ngarn table name.',
)
def create_table(name):
    asyncio.run(utils.create_table(name))


script.add_command(run)
script.add_command(create_table)
