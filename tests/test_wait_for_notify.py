import asyncio
from concurrent.futures import ProcessPoolExecutor

import pytest
from mod_ngarn.connection import get_connection
from mod_ngarn.utils import wait_for_notify, shutdown
import sys


@pytest.mark.asyncio
async def test_wait_for_notify_should_listen_to_channel_and_put_to_queue_when_notify(
    event_loop
):
    queue_table = "mod_ngarn_job"
    q = asyncio.Queue(loop=event_loop)
    cnx = await get_connection()
    await wait_for_notify(queue_table, q)
    await cnx.execute("NOTIFY mod_ngarn_job")
    channel = await q.get()
    assert channel == queue_table


@pytest.mark.asyncio
async def test_shutdown_should_exit(event_loop):
    q = asyncio.Queue(loop=event_loop)
    with pytest.raises(SystemExit):
        await q.put("Notification")
        await shutdown(q)
