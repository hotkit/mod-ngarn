"""Simple async worker"""

__version__ = "3.5"

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
    queue_table_schema, queue_table_name = (
        utils.sql_table_name(queue_table).replace('"', "").split(".")
    )
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
@click.option("--limit", default=300, help="Limit jobs (Default: 300)")
@click.option(
    "--max-delay",
    type=float,
    help="Max delay for failed jobs (seconds) (Default: None)",
)
def run_listen(queue_table, limit, max_delay):
    """
    Run mod-ngarn job in a loop.
    Regularly poling and listening for a job
    """
    queue_table_schema, queue_table_name = (
        utils.sql_table_name(queue_table).replace('"', "").split(".")
    )
    job_runner = JobRunner()
    loop = asyncio.get_event_loop()
    #Create a notification queue
    notification_queue = asyncio.Queue(loop=loop)
    if max_delay:
        max_delay = float(max_delay)
    #Add long running task to subscribe to notification.
    #Publish event to queue when there is a notification
    loop.create_task(
        utils.wait_for_notify_longterm(queue_table_schema, queue_table_name, notification_queue)
    )
    #Add a poller that regularly checks for job in table
    #Example use-case: During startup, or cronjobs
    loop.create_task(
        utils.pending_job_poller(queue_table_schema, queue_table_name, notification_queue)
    )
    #Start the runner, which only calls run() if there is a message on the queue
    loop.create_task(
        job_runner.run_listen(queue_table_schema, queue_table_name, limit, max_delay, notification_queue)
    )
    loop.run_forever()

@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "public.modngarn_job"))',
    default=os.getenv("DBTABLE", "public.modngarn_job"),
)
def create_table(queue_table):
    """Create mod-ngarn queue table"""
    queue_table_schema, queue_table_name = (
        utils.sql_table_name(queue_table).replace('"', "").split(".")
    )
    asyncio.run(utils.create_table(queue_table_schema, queue_table_name))


@click.command()
@click.option(
    "--queue-table",
    help='Queue table name (Default: os.getenv("DBTABLE", "public.modngarn_job"))',
    default=os.getenv("DBTABLE", "public.modngarn_job"),
)
def wait_for_notify(queue_table):
    """Wait and listening for NOTIFY"""
    queue_table_schema, queue_table_name = (
        utils.sql_table_name(queue_table).replace('"', "").split(".")
    )
    loop = asyncio.get_event_loop()
    is_pending_job_exists = loop.run_until_complete(
        utils.is_pending_job_exists(queue_table_schema, queue_table_name))

    if not is_pending_job_exists:
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
    queue_table_schema, queue_table_name = (
        utils.sql_table_name(queue_table).replace('"', "").split(".")
    )
    asyncio.run(utils.delete_executed_job(queue_table_schema, queue_table_name))


script.add_command(run)
script.add_command(create_table)
script.add_command(wait_for_notify)
script.add_command(delete_job)
script.add_command(run_listen)
