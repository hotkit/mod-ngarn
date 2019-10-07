"""Simple async worker"""

__version__ = "3.4"

import asyncio
import os
import sys

import click

from . import utils
from .worker import JobRunner

global script
global run
global create_table
global delete_job


@click.group()
def script():
    pass


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "public.modngarn_job"))',
    default=os.getenv("DBTABLE", "public.modngarn_job"),
)
@click.option("--limit", default=300, help="Limit jobs (Default: 300)")
@click.option(
    "--max-delay",
    type=float,
    help="Max delay for failed jobs (seconds) (Default: None)",
)
def run(queue_table, limit, max_delay):
    """Run mod-ngarn job"""
    queue_table_schema, queue_table_name = queue_table.splt(".")
    job_runner = JobRunner()
    loop = asyncio.get_event_loop()
    if max_delay:
        max_delay = float(max_delay)
    loop.run_until_complete(
        job_runner.run(queue_table_schema, queue_table_name, limit, max_delay)
    )


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "public.modngarn_job"))',
    default=os.getenv("DBTABLE", "public.modngarn_job"),
)
def create_table(queue_table):
    """Create mod-ngarn queue table"""
    queue_table_schema, queue_table_name = queue_table.splt(".")
    asyncio.run(utils.create_table(queue_table_schema, queue_table_name))


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "public.modngarn_job"))',
    default=os.getenv("DBTABLE", "public.modngarn_job"),
)
def wait_for_notify(queue_table):
    """Wait and listening for NOTIFY"""
    queue_table_schema, queue_table_name = queue_table.splt(".")
    loop = asyncio.get_event_loop()
    notification_queue = asyncio.Queue(loop=loop)
    loop.create_task(
        utils.wait_for_notify(queue_table_schema, queue_table_name, notification_queue)
    )
    loop.run_until_complete(utils.shutdown(notification_queue))
    loop.run_forever()


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "public.modngarn_job"))',
    default=os.getenv("DBTABLE", "public.modngarn_job"),
)
def delete_job(queue_table):
    """Delete executed task"""
    asyncio.run(utils.call_delete_executed_job(queue_table))


script.add_command(run)
script.add_command(create_table)
script.add_command(wait_for_notify)
script.add_command(delete_job)
