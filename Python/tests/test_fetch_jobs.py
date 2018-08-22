from worker import fetch_jobs
import pytest
from connection import get_connection

test_data = [{
    "id": "id-01",
    "fn_name": "init",
    "args": "[1,2]",
    "kwargs": None,
    "priority": 3,
    "created": "2018-03-18",
    "scheduled": None,
    "started": None,
    "executed": None,
    "cancelled": None,
    "result": None 
}, {
    "id": "id-02",
    "fn_name": "init",
    "args": "[1,2]",
    "kwargs": None,
    "priority": 1,
    "created": "2018-03-18",
    "scheduled": None,
    "started": None,
    "executed": None,
    "cancelled": None,
    "result": None 
}]

async def init_jobs():
    conn = await get_connection()
    insert_query = """INSERT INTO modngarn_job VALUES (
            $1, $2, $3, $4, $5, '2018-03-18', $6, $7, $8, $9, $10
    );"""
    for data in test_data:
        await conn.execute(insert_query, 
            data["id"],
            data["fn_name"],
            data["args"],
            data["kwargs"],
            data["priority"],
            data["scheduled"],
            data["started"],
            data["executed"],
            data["cancelled"],
            data["result"]
        )


@pytest.mark.asyncio
async def test_fetch_jobs():
    conn = await get_connection()
    await init_jobs()
    values = await fetch_jobs(conn)
    await conn.close()
    job = dict(values[0])
    expected_jobs = [{
        "id": "id-01",
        "fn_name": "init",
        "args": "[1,2]",
        "kwargs": None,
        "priority": 3,
        "created": "2018-03-18",
        "scheduled": None,
        "started": None,
        "executed": None,
        "cancelled": None,
        "result": None 
    }, {
        "id": "id-02",
        "fn_name": "init",
        "args": "[1,2]",
        "kwargs": None,
        "priority": 1,
        "created": "2018-03-18",
        "scheduled": None,
        "started": None,
        "executed": None,
        "cancelled": None,
        "result": None 
    }]
    assert job['id'] == expected_jobs[0]['id']
