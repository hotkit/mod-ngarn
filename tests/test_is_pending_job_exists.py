import sys
import pytest

import asyncio

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table, is_pending_job_exists
from datetime import datetime, timedelta, timezone

from tests.utils import insert_job


@pytest.mark.asyncio
async def test_is_job_exists_should_return_true_when_has_non_excuted_job_exists(
    event_loop
):
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
    cnx = await get_connection()
    await cnx.execute(f"TRUNCATE TABLE {queue_table}")
    await cnx.execute(f"TRUNCATE TABLE {queue_table}_error")

    await insert_job(
        cnx, queue_table, "job-1", "tests.utils.async_dummy_job", executed_time=None
    )

    result = await is_pending_job_exists("public", "modngarn_job")
    assert result == True


@pytest.mark.asyncio
async def test_is_job_exists_should_return_false_when_has_no_non_excuted_job_exists(
    event_loop
):
    queue_table = "public.modngarn_job"
    await create_table("public", "modngarn_job")
    cnx = await get_connection()
    await cnx.execute(f"TRUNCATE TABLE {queue_table}")
    await cnx.execute(f"TRUNCATE TABLE {queue_table}_error")

    await insert_job(
        cnx, queue_table, "job-1", "tests.utils.async_dummy_job", executed_time=datetime.utcnow().replace(tzinfo=timezone.utc)
    )

    await insert_job(
        cnx, queue_table, "job-2", "tests.utils.async_dummy_job", scheduled=datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(days=2)
    )

    result = await is_pending_job_exists("public", "modngarn_job")
    assert result == False
