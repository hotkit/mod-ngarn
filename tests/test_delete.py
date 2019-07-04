import asyncio
import sys
from concurrent.futures import ProcessPoolExecutor

import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table, delete_executed_job, add_delete_job
from mod_ngarn.worker import Job, JobRunner
from datetime import datetime
from tests.utils import insert_job

async def async_dummy_job(text):
    return text

@pytest.mark.asyncio
async def test_delete_executed_job_successfully(event_loop):
    queue_table = "modngarn_job"
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f"TRUNCATE TABLE {queue_table}")
    await cnx.execute(f"TRUNCATE TABLE {queue_table}_error")

    await insert_job(cnx, queue_table, 'job-1', 'tests.utils.async_dummy_job', None, 'hello')
    await insert_job(cnx, queue_table, 'job-2', 'tests.utils.async_dummy_job', datetime.now(), 'hello')

    result = await delete_executed_job(cnx, queue_table)
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "1"
    res = await cnx.fetch(f"SELECT * FROM {queue_table}")
    assert len(res) == 1
    assert res[0]["id"] == "job-1"
    await cnx.close()

@pytest.mark.asyncio
async def test_add_delete_job_and_run_successfully(event_loop):
    queue_table = "modngarn_job"
    log_table_name = f"{queue_table}_error"
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f"TRUNCATE TABLE {queue_table}")
    await cnx.execute(f"TRUNCATE TABLE {queue_table}_error")

    await add_delete_job(cnx, queue_table)

    job = await cnx.fetchrow(
        """SELECT * FROM {queue_table}""".format(queue_table=queue_table)
    )

    assert job['scheduled'] == None
    assert job['fn_name'] == 'mod_ngarn.utils.delete_controller'
    assert job['id'] == 'delete_executed_task'

    await insert_job(cnx, queue_table, 'job-1', 'tests.utils.async_dummy_job', None, 'hello')
    await insert_job(cnx, queue_table, 'job-2', 'tests.utils.async_dummy_job', None, 'hello')
    await insert_job(cnx, queue_table, 'job-3', 'tests.utils.async_error_job', None, 'hello')

    job_runner = JobRunner()
    await job_runner.run(queue_table, 5, 1)

    jobs = await cnx.fetch(
        """SELECT * FROM {queue_table}""".format(queue_table=queue_table)
    )

    assert len(jobs) == 2

@pytest.mark.asyncio
async def test_can_add_delete_job_periodically(event_loop):
    queue_table = "modngarn_job"
    log_table_name = f"{queue_table}_error"
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f"TRUNCATE TABLE {queue_table}")
    await cnx.execute(f"TRUNCATE TABLE {queue_table}_error")

    await insert_job(cnx, queue_table, 'job-1', 'tests.utils.async_dummy_job', None, 'hello')
    await insert_job(cnx, queue_table, 'job-2', 'tests.utils.async_dummy_job', None, 'hello')
    await insert_job(cnx, queue_table, 'job-3', 'tests.utils.async_error_job', None, 'hello')
    await add_delete_job(cnx, queue_table, 2)

    job_runner = JobRunner()
    await job_runner.run(queue_table, 5, 1)

    jobs = await cnx.fetch(
        """SELECT * FROM {queue_table}""".format(queue_table=queue_table)
    )

    assert len(jobs) == 2

    deleted_job = await cnx.fetchrow(
        """SELECT * FROM {queue_table} WHERE id =$1""".format(queue_table=queue_table), 'delete_executed_task'
    )

    assert deleted_job['executed'] == None