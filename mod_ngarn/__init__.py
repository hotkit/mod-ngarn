"""Simple async worker"""

__version__ = "2.3"

import asyncio

import click

from .worker import JobRunner
from .utils import setup_table, escape_table_name
import os

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
    '--table',
    default=escape_table_name(os.getenv('DBTABLE', 'modngarn_job')),
    help='Database table name.',
)
def init_table(table):
    asyncio.run(setup_table(table))


script.add_command(run)
script.add_command(init_table)
