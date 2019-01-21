"""Simple async worker"""

__version__ = "2.5"

import asyncio
import os

import click

from . import utils
from .worker import JobRunner

global script
global run
global create_table


@click.group()
def script():
    pass


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "modngarn_job"))',
    default=utils.escape_table_name(os.getenv("DBTABLE", "modngarn_job")),
)
@click.option("--limit", default=300, help="Limit jobs (Default: 300)")
def run(queue_table, limit):
    job_runner = JobRunner()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(job_runner.run(queue_table, limit=limit))


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "modngarn_job"))',
    default=utils.escape_table_name(os.getenv("DBTABLE", "modngarn_job")),
)
def create_table(queue_table):
    asyncio.run(utils.create_table(queue_table))


script.add_command(run)
script.add_command(create_table)
