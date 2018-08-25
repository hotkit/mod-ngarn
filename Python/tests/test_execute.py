from worker import execute
import pytest
from connection import get_connection
from asyncpg import Record


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