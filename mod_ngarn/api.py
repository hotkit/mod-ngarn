import asyncio
import functools
import logging
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List

import asyncpg

from dataclasses import dataclass, field
from .utils import import_fn


async def add_job(
    cnx: asyncpg.Connection,
    job_id: str,
    fn_name: str,
    schedule_time: datetime =None,
    args: list =[],
    kwargs: dict ={}
):
    await cnx.execute(
        """
        INSERT INTO public.modngarn_job (id, fn_name, scheduled, args, kwargs)
        VALUES ($1,  $2, $3, $4, $5) RETURNING *;
        """,
        job_id, fn_name, schedule_time, args, kwargs
    )