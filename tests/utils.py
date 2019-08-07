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
    *args,
    **kwargs
) -> str:
    return await connection.execute(
        """
        INSERT INTO {queue_table} (id, fn_name, args, kwargs, executed) 
        VALUES ($1, $2, $3, $4, $5)
        """.format(
            queue_table=table_name
        ),
        id,
        function_name,
        args,
        kwargs,
        executed_time,
    )
