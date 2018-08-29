import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.worker import record_success_result


@pytest.mark.asyncio
async def test_record_success_result_should_update_db():
    cnx = await get_connection()
    insert_query = """
        INSERT INTO modngarn_job (id, fn_name, args, kwargs, scheduled, executed) 
        VALUES 
            ('job-1', 'asycio.sleep', '[2]', '{}', NULL, '2018-08-10'),
            ('job-2', 'asycio.sleep', '[2]', '{}', NULL, '2018-08-13'),
            ('job-3', 'asycio.sleep', '[2]', '{}', NULL, NULL);
    """
    await cnx.execute(insert_query)
    await record_success_result(cnx, {'id': 'job-3'}, '3.00', '"Cool"')
    j = await cnx.fetchrow('SELECT * FROM modngarn_job WHERE id=$1', 'job-3')
    assert j['result'] == '"Cool"'
    await cnx.execute('TRUNCATE TABLE modngarn_job;')
    await cnx.close()
