import sys

import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table, delete_executed_job

from datetime import datetime, timedelta, timezone
from tests.utils import insert_job


async def async_dummy_job(text):
    return text


@pytest.mark.asyncio
async def test_delete_executed_job_successfully():
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
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
        datetime.utcnow().replace(tzinfo=timezone.utc),
        "hello",
    )

    result = await delete_executed_job(queue_table, cnx=cnx)
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "1"
    job = await cnx.fetch(f"SELECT * FROM {queue_table}")
    assert len(job) == 1
    assert job[0]["id"] == "job-1"
    await cnx.close()


@pytest.mark.asyncio
async def test_delete_executed_job_with_keep_period():
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
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
        datetime.utcnow().replace(tzinfo=timezone.utc),
        "hello",
    )

    await insert_job(
        cnx,
        queue_table,
        "job-3",
        "tests.utils.async_dummy_job",
        datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=2),
        "hello",
    )

    result = await delete_executed_job(queue_table, cnx=cnx, keep_period_day=1)
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "1"
    job = await cnx.fetch(f"SELECT * FROM {queue_table} ORDER BY id;")
    assert len(job) == 2
    assert job[0]["id"] == "job-1"
    assert job[1]["id"] == "job-2"
    await cnx.close()


@pytest.mark.asyncio
async def test_delete_executed_job_with_batch_size_lower_than_executed_job():
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
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
        datetime.utcnow().replace(tzinfo=timezone.utc),
        "hello",
    )

    await insert_job(
        cnx,
        queue_table,
        "job-3",
        "tests.utils.async_dummy_job",
        datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=2),
        "hello",
    )

    result = await delete_executed_job(queue_table, cnx=cnx, batch_size=1)
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "1"
    res = await cnx.fetch(f"SELECT * FROM {queue_table} ORDER BY fn_name DESC, id ASC;")
    assert len(res) == 3
    assert res[0]["id"] == "job-1"
    assert res[1]["id"] == "job-2"
    assert res[2]["fn_name"] == "mod_ngarn.utils.delete_executed_job"
    assert res[2]["scheduled"] == None
    await cnx.close()


@pytest.mark.asyncio
async def test_delete_executed_job_with_repeat_true():
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
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
        datetime.utcnow().replace(tzinfo=timezone.utc),
        "hello",
    )

    await insert_job(
        cnx,
        queue_table,
        "job-3",
        "tests.utils.async_dummy_job",
        datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=2),
        "hello",
    )

    result = await delete_executed_job(
        queue_table, cnx=cnx, repeat=True, scheduled_day=30
    )
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "2"
    res = await cnx.fetch(
        f"SELECT * FROM {queue_table} ORDER BY fn_name DESC, id ASC;;"
    )
    assert len(res) == 2
    assert res[0]["id"] == "job-1"
    assert res[1]["fn_name"] == "mod_ngarn.utils.delete_executed_job"
    assert (res[1]["scheduled"] - res[1]["created"]).days == 29
    await cnx.close()


@pytest.mark.asyncio
async def test_delete_executed_job_with_repeat_true_and_batch():
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
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
        datetime.utcnow().replace(tzinfo=timezone.utc),
        "hello",
    )

    await insert_job(
        cnx,
        queue_table,
        "job-3",
        "tests.utils.async_dummy_job",
        datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=2),
        "hello",
    )

    result = await delete_executed_job(
        queue_table, cnx=cnx, repeat=True, scheduled_day=30, batch_size=1
    )
    operation, effected_row = result.split(" ")
    assert operation == "DELETE"
    assert effected_row == "1"
    res = await cnx.fetch(
        f"SELECT * FROM {queue_table} ORDER BY fn_name DESC, id ASC;;"
    )
    assert len(res) == 3
    assert res[0]["id"] == "job-1"
    assert res[1]["id"] == "job-2"
    assert res[2]["fn_name"] == "mod_ngarn.utils.delete_executed_job"
    assert res[2]["scheduled"] == None
    await cnx.close()
