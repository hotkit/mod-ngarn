"""Simple async worker"""

__version__ = "2.3"

import asyncio

import click

from .worker import JobRunner

global script
global run


@click.group()
def script():
    pass


@click.command()
def run():
    job_runner = JobRunner()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(job_runner.run())


script.add_command(run)
