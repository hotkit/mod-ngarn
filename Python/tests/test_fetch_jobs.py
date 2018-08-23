from worker import fetch_jobs
import pytest
from connection import get_connection


@pytest.mark.asyncio
async def test_fetch_jobs_should_be_able_to_fetch_not_processed_jobs():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, kwargs, scheduled, executed) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', '{}', NULL, '2018-08-10'),
            ('job-2', 'asycio.sleep', '[2]', '{}', NULL, '2018-08-13'),
            ('job-3', 'asycio.sleep', '[2]', '{}', NULL, NULL);
    """
    await cnx.execute(insert_query)
    jobs = await fetch_jobs(cnx)
    assert len(jobs) == 1
    assert jobs[0]['id'] == 'job-3'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_jobs_should_be_able_to_fetch_correct_scheduled_jobs():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, kwargs, scheduled, executed) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', '{}', NOW() + INTERVAL '10 minutes', NULL),
            ('job-2', 'asycio.sleep', '[2]', '{}', NOW() - INTERVAL '10 minutes', NULL);
    """
    await cnx.execute(insert_query)
    jobs = await fetch_jobs(cnx)
    assert len(jobs) == 1
    assert jobs[0]['id'] == 'job-2'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_jobs_should_be_able_to_fetch_correct_priorities():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, priority) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', 10),
            ('job-2', 'asycio.sleep', '[2]', 2),
            ('job-3', 'asycio.sleep', '[2]', 1);
    """
    await cnx.execute(insert_query)
    jobs = await fetch_jobs(cnx)
    assert len(jobs) == 3
    assert jobs[0]['id'] == 'job-3'
    assert jobs[1]['id'] == 'job-2'
    assert jobs[2]['id'] == 'job-1'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()
