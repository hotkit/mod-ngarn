import os
from datetime import datetime
from typing import Callable, Optional, Union

import asyncpg

from .utils import sql_table_name, get_fn_name


async def add_job(
    cnx: asyncpg.Connection,
    queue_table: str,
    job_id: str,
    func: Union[str, Callable],
    schedule_time: Optional[datetime] = None,
    priority: int = 0,
    args: list = [],
    kwargs: dict = {},
) -> asyncpg.Record:
    fn_name = await get_fn_name(func)
    return await cnx.fetchrow(
        """
        INSERT INTO {queue_table} (id, fn_name, priority, scheduled, args, kwargs)
        VALUES ($1, $2, $3, $4, $5, $6) RETURNING *;
        """.format(
            queue_table=sql_table_name(queue_table)
        ),
        job_id,
        fn_name,
        priority,
        schedule_time,
        args,
        kwargs,
    )
