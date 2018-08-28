import pytest
from asyncpg import Record

from mod_ngarn.connection import get_connection
from mod_ngarn.worker import execute


@pytest.mark.asyncio
async def test_execute_can_execute_sync_fn():
    job = {
        'id': 'job-1',
        'fn_name': 'tests.test_import_fn.sync_ret',
        'args': ["Test"],
        'kwargs': {}
    }
    result = await execute(job)
    assert result == "Test"


@pytest.mark.asyncio
async def test_execute_can_execute_async_fn():
    job = {
        'id': 'job-1',
        'fn_name': 'tests.test_import_fn.async_ret',
        'args': ["Test"],
        'kwargs': {}
    }
    result = await execute(job)
    assert result == "Test"
