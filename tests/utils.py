from asyncpg import Connection
from datetime import datetime


async def async_dummy_job(text) -> str:
    return text


async def async_error_job(text) -> None:
    raise Exception()


async def insert_job(
    connection: Connection,
    table_name: str,
    id: str,
    function_name: str,
    executed_time: datetime = None,
    scheduled: datetime = None,
    args: list = [],
    kwargs: dict = {}
) -> str:
    return await connection.execute(
        """
        INSERT INTO {queue_table} (id, fn_name, args, kwargs, executed, scheduled)
        VALUES ($1, $2, $3, $4, $5, $6)
        """.format(
            queue_table=table_name
        ),
        id,
        function_name,
        args,
        kwargs,
        executed_time,
        scheduled,
    )
