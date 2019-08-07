import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table, exec_create_log_table, exec_create_table
from mod_ngarn.worker import JobRunner
from tests.utils import insert_job
from datetime import datetime


@pytest.mark.asyncio
async def test_can_create_table_and_fetch_job_with_specific_name():
    cnx = await get_connection()
    table_name = "public.modngarn_job"
    await create_table(table_name)
    insert_query = """
        INSERT INTO {table_name} (id, fn_name, priority, args, kwargs, scheduled, executed)
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
    assert job.id == "job-3"
    await cnx.execute(f"DROP TABLE {table_name};")
    await cnx.execute(f"DROP TABLE {table_name}_error;")
    await cnx.close()


@pytest.mark.asyncio
async def test_can_exec_create_table_and_fetch_job_with_specific_name():
    cnx = await get_connection()
    table_name = "public.modngarn_job"
    await exec_create_table(cnx, table_name)
    insert_query = """
        INSERT INTO {table_name} (id, fn_name, priority, args, kwargs, scheduled, executed)
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
    assert job.id == "job-3"
    await cnx.execute(f"DROP TABLE {table_name};")
    await cnx.close()


@pytest.mark.asyncio
async def test_can_create_log_table_when_create_table():
    cnx = await get_connection()
    table_name = "public.modngarn_job"
    log_table_name = f"{table_name}_error"

    await create_table(table_name)
    await cnx.execute(f"TRUNCATE TABLE {table_name};")
    await cnx.execute(f"TRUNCATE TABLE {log_table_name};")

    insert_query = """
        INSERT INTO {log_table_name} (id, fn_name, args, kwargs, message, posted)
        VALUES
            ('job-1', 'asycio.sleep', '[2]', '{{}}', 'error occur', '2017-08-10'),
            ('job-2', 'asycio.sleep', '[2]', '{{}}', 'error occur', '2018-09-13'),
            ('job-3', 'asycio.sleep', '[2]', '{{}}', 'error occur', '2019-10-10');
    """.format(
        log_table_name=log_table_name
    )
    await cnx.execute(insert_query)

    result = await cnx.fetch(
        """SELECT * FROM {log_table_name}""".format(log_table_name=log_table_name)
    )
    assert len(result) == 3
    await cnx.execute(f"DROP TABLE {table_name};")
    await cnx.execute(f"DROP TABLE {table_name}_error;")
    await cnx.close()


@pytest.mark.asyncio
async def test_can_exec_create_log_table_when_create_table():
    cnx = await get_connection()
    table_name = "public.modngarn_job"
    log_table_name = f"{table_name}_error"

    await exec_create_log_table(cnx, log_table_name)
    await cnx.execute(f"TRUNCATE TABLE {log_table_name};")

    insert_query = """
        INSERT INTO {log_table_name} (id, fn_name, args, kwargs, message, posted)
        VALUES
            ('job-1', 'asycio.sleep', '[2]', '{{}}', 'error occur', '2017-08-10'),
            ('job-2', 'asycio.sleep', '[2]', '{{}}', 'error occur', '2018-09-13'),
            ('job-3', 'asycio.sleep', '[2]', '{{}}', 'error occur', '2019-10-10');
    """.format(
        log_table_name=log_table_name
    )
    await cnx.execute(insert_query)

    result = await cnx.fetch(
        """SELECT * FROM {log_table_name}""".format(log_table_name=log_table_name)
    )
    assert len(result) == 3
    await cnx.execute(f"DROP TABLE {table_name}_error;")
    await cnx.close()


@pytest.mark.asyncio
async def test_keep_log_in_log_table_when_task_failed():
    cnx = await get_connection()
    table_name = "public.modngarn_job"
    log_table_name = f"{table_name}_error"

    await create_table(table_name)
    await cnx.execute(f"TRUNCATE TABLE {table_name};")
    await cnx.execute(f"TRUNCATE TABLE {log_table_name};")

    await insert_job(cnx, table_name, "job-1", "asyncio.sleep", datetime.now())
    await insert_job(cnx, table_name, "job-2", "asyncio.sleep", datetime.now())
    await insert_job(cnx, table_name, "job-3", "tests.utils.async_error_job", None)

    job_runner = JobRunner()
    await job_runner.run(table_name, 5, 1)

    jobs = await cnx.fetch(
        """SELECT * FROM {log_table_name}""".format(log_table_name=log_table_name)
    )
    assert len(jobs) == 1
    assert jobs[0]["processed_time"]
    assert jobs[0]["posted"]

    # Keep ervery time error happen event it come from same task
    await cnx.fetch(
        """UPDATE {table_name} SET scheduled = null where id = 'job-3'""".format(
            table_name=table_name
        )
    )
    await job_runner.run(table_name, 5, 1)
    jobs = await cnx.fetch(
        """SELECT * FROM {log_table_name}""".format(log_table_name=log_table_name)
    )
    assert len(jobs) == 2

    await cnx.execute(f"TRUNCATE TABLE {log_table_name};")
    await cnx.execute(f"DROP TABLE {table_name};")
    await cnx.execute(f"DROP TABLE {table_name}_error;")
    await cnx.close()


@pytest.mark.asyncio
async def test_can_skipped_create_table_process_when_table_exists():
    cnx = await get_connection()
    table_name = "public.test_init_table"
    await create_table(table_name)
    await create_table(table_name)
    await cnx.execute(f"DROP TABLE {table_name};")
    await cnx.execute(f"DROP TABLE {table_name}_error;")
    await cnx.close()
