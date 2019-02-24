import os

import pytest
from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table
from mod_ngarn.worker import JobRunner


@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_not_processed_job():
    queue_table = 'public.modngarn_job'
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    insert_query = """
        INSERT INTO {queue_table} (id, fn_name, args, kwargs, scheduled, executed)
        VALUES
            ('job-1', 'asycio.sleep', '[2]', '{{}}', NULL, '2018-08-10'),
            ('job-2', 'asycio.sleep', '[2]', '{{}}', NULL, '2018-08-13'),
            ('job-3', 'asycio.sleep', '[2]', '{{}}', NULL, NULL);
    """.format(
        queue_table=queue_table
    )
    await cnx.execute(insert_query)
    job_runner = JobRunner()
    job = await job_runner.fetch_job(cnx, queue_table, None)
    assert job.id == 'job-3'
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_correct_scheduled_job():
    queue_table = 'public.modngarn_job'
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    insert_query = """
        INSERT INTO {queue_table} (id, fn_name, args, kwargs, scheduled, executed)
        VALUES
            ('job-1', 'asycio.sleep', '[2]', '{{}}', NOW() + INTERVAL '10 minutes', NULL),
            ('job-2', 'asycio.sleep', '[2]', '{{}}', NOW() - INTERVAL '10 minutes', NULL);
    """.format(
        queue_table=queue_table
    )
    await cnx.execute(insert_query)
    job_runner = JobRunner()
    job = await job_runner.fetch_job(cnx, queue_table, None)
    assert job.id == 'job-2'
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_correct_priorities():
    queue_table = 'public.modngarn_job'
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    insert_query = """
        INSERT INTO {queue_table} (id, fn_name, args, priority)
        VALUES
            ('job-1', 'asycio.sleep', '[2]', 10),
            ('job-2', 'asycio.sleep', '[2]', 2),
            ('job-3', 'asycio.sleep', '[2]', 1);
    """.format(
        queue_table=queue_table
    )
    await cnx.execute(insert_query)
    job_runner = JobRunner()
    job = await job_runner.fetch_job(cnx, queue_table, None)
    assert job.id == 'job-3'
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_job_should_be_fetch_only_not_claimed_job():
    queue_table = 'public.modngarn_job'
    await create_table(queue_table)
    insert_query = """
        INSERT INTO {queue_table} (id, fn_name, args, priority)
        VALUES
            ('job-1', 'asycio.sleep', '[2]', 10),
            ('job-2', 'asycio.sleep', '[2]', 2),
            ('job-3', 'asycio.sleep', '[2]', 1);
    """.format(
        queue_table=queue_table
    )
    cnx = await get_connection()
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    await cnx.execute(insert_query)

    # First worker fetch_job
    cnx1 = await get_connection()
    job_runner = JobRunner()
    tx1 = cnx1.transaction()
    await tx1.start()
    job1 = await job_runner.fetch_job(cnx1, queue_table, None)

    # Second worker fetch_job
    cnx2 = await get_connection()
    tx2 = cnx2.transaction()
    await tx2.start()
    job2 = await job_runner.fetch_job(cnx2, queue_table, None)

    assert job1.id == 'job-3'
    assert job2.id == 'job-2'
    await tx1.commit()
    await tx2.commit()
    await cnx.execute(f'TRUNCATE TABLE {queue_table};')
    await cnx.close()
    await cnx1.close()
    await cnx2.close()
