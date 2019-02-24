import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table
from mod_ngarn.worker import JobRunner


@pytest.mark.asyncio
async def test_can_create_table_and_fetch_job_with_specific_name():
    cnx = await get_connection()
    table_name = 'test_init_table'
    await create_table(table_name)
    insert_query = """
        INSERT INTO "{table_name}" (id, fn_name, priority, args, kwargs, scheduled, executed)
        VALUES
            ('job-1', 'asycio.sleep', 1, '[2]', '{{}}', NULL, '2018-08-10'),
            ('job-2', 'asycio.sleep', 2, '[2]', '{{}}', NULL, '2018-08-13'),
            ('job-3', 'asycio.sleep', 3, '[2]', '{{}}', NULL, NULL);
    """.format(
        table_name=table_name
    )
    await cnx.execute(insert_query)
    job_runner = JobRunner()
    job = await job_runner.fetch_job(cnx, table_name, None)
    assert job.id == 'job-3'
    await cnx.execute(f'TRUNCATE TABLE {table_name};')
    await cnx.close()
