import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.worker import fetch_job


@pytest.mark.asyncio
async def test_insert_job_not_update():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, kwargs, scheduled, executed) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', '{}', NULL, '2018-08-10'),
            ('job-2', 'asycio.sleep', '[2]', '{}', NULL, '2018-08-13'),
            ('job-3', 'asycio.sleep', '[2]', '{}', NULL, NULL);
    """
    await cnx.execute(insert_query)
    job = await fetch_job(cnx)
    assert job['id'] == 'job-3'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_correct_scheduled_job():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, kwargs, scheduled, executed) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', '{}', NOW() + INTERVAL '10 minutes', NULL),
            ('job-2', 'asycio.sleep', '[2]', '{}', NOW() - INTERVAL '10 minutes', NULL);
    """
    await cnx.execute(insert_query)
    job = await fetch_job(cnx)
    assert job['id'] == 'job-2'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_correct_priorities():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, priority) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', 10),
            ('job-2', 'asycio.sleep', '[2]', 2),
            ('job-3', 'asycio.sleep', '[2]', 1);
    """
    await cnx.execute(insert_query)
    job = await fetch_job(cnx)
    assert job['id'] == 'job-3'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()


@pytest.mark.asyncio
async def test_fetch_job_should_be_fetch_only_not_claimed_job():
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, priority) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', 10),
            ('job-2', 'asycio.sleep', '[2]', 2),
            ('job-3', 'asycio.sleep', '[2]', 1);
    """
    cnx = await get_connection()
    await cnx.execute(insert_query)

    # First worker fetch_job
    cnx1 = await get_connection()
    tx1 = cnx1.transaction()
    await tx1.start()
    job1 = await fetch_job(cnx1)
    

    # Second worker fetch_job
    cnx2 = await get_connection()
    tx2 = cnx2.transaction()
    await tx2.start()
    job2 = await fetch_job(cnx2)
    
    assert job1['id'] == 'job-3'
    assert job2['id'] == 'job-2'
    await tx1.commit()
    await tx2.commit()
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()
    await cnx1.close()
    await cnx2.close()
