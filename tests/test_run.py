import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.worker import run


@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_correct_priorities():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, priority) 
        VALUES 
            ('job-1', 'print', '[1]', 10),
            ('job-2', 'print', '["Hello"]', 2),
            ('job-3', 'print', '[1]', 1);
    """
    await cnx.execute(insert_query)
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 3
    await run()
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 2
    await run()
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 1
    await run()
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 0

    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()

@pytest.mark.asyncio
async def test_fetch_job_should_be_able_to_fetch_correct_priorities():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, priority) 
        VALUES 
            ('job-1', 'print', '[1]', 10),
            ('job-2', 'print', '["Hello"]', 2),
            ('job-3', 'print', '[1]', 1);
    """
    await cnx.execute(insert_query)
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 3
    await run()
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 2
    await run()
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 1
    await run()
    assert await cnx.fetchval('SELECT COUNT(*) FROM modngarn_job WHERE executed IS NULL') == 0

    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()