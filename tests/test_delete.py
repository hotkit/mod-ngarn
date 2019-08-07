import sys

import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table, delete_executed_job

from datetime import datetime
from tests.utils import insert_job


async def async_dummy_job(text):
    return text


@pytest.mark.asyncio
async def test_delete_executed_job_successfully(event_loop):
    queue_table = "public.modngarn_job"
    await create_table(queue_table)
    cnx = await get_connection()
    await cnx.execute(f"TRUNCATE TABLE {queue_table}")
    await cnx.execute(f"TRUNCATE TABLE {queue_table}_error")

    await insert_job(
        cnx, queue_table, "job-1", "tests.utils.async_dummy_job", None, "hello"
    )
    await insert_job(
        cnx,
        queue_table,
        "job-2",
        "tests.utils.async_dummy_job",
        datetime.now(),
        "hello",
    )

    result = await delete_executed_job(queue_table)
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "1"
    res = await cnx.fetch(f"SELECT * FROM {queue_table}")
    assert len(res) == 1
    assert res[0]["id"] == "job-1"
    await cnx.close()
