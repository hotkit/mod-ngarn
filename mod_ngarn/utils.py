import asyncio
import asyncpg
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from inspect import getmembers, getmodule, ismethod
from typing import Callable, Union
import uuid

from asyncpg.connection import Connection

from .connection import DBConnection


class ImportNotFoundException(Exception):
    pass


class ModuleNotfoundException(Exception):
    pass


def sql_table_name(queue_table: str) -> str:
    quote_table_name = [f'"{x}"' for x in queue_table.replace('"', "").split(".")]
    table_name = (
        ['"public"'] + quote_table_name
        if len(quote_table_name) == 1
        else quote_table_name
    )
    return (".").join(table_name)


async def get_fn_name(func: Union[str, Callable]) -> str:
    try:
        if isinstance(func, str):
            return func
        if ismethod(func):
            module_name = get_fn_name(dict(getmembers(func))["__self__"])
        else:
            module_name = getmodule(func).__name__
        name = func.__name__
        return ".".join([module_name, name])
    except AttributeError as e:
        raise ModuleNotfoundException(e)


async def import_fn(fn_name) -> Callable:
    access_path = fn_name.split(".")
    module = None
    try:
        for index in range(1, len(access_path)):
            try:
                # import top level module
                module_name = ".".join(access_path[:-index])
                module = __import__(module_name)
            except ImportError:
                continue
            else:
                for step in access_path[1:-1]:  # walk down it
                    module = getattr(module, step)
                break
        if module:
            return getattr(module, access_path[-1])
        else:
            return globals()["__builtins__"][fn_name]
    except KeyError as e:
        raise ImportNotFoundException(e)


async def is_migration_executed(
    cnx: asyncpg.connection,
    migrate_file: str,
    queue_table_schema: str,
    queue_table_name: str,
) -> bool:
    migration_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.mod_ngarn_migration');
        """
    )
    if not migration_table:
        return False

    return await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM public.mod_ngarn_migration
            WHERE migrate_file = $1 AND queue_table = $2);
        """,
        migrate_file,
        f"{queue_table_schema}.{queue_table_name}",
    )


async def migrate(
    cnx: asyncpg.connection,
    migrate_file: str,
    queue_table_schema: str,
    queue_table_name: str,
) -> None:
    with open(migrate_file) as f:
        sql = f.read()
        sql = sql.replace("{queue_table_schema}", queue_table_schema)
        sql = sql.replace("{queue_table_name}", queue_table_name)
        await cnx.execute(sql)
        await cnx.execute(
            """
            INSERT INTO public.mod_ngarn_migration(migrate_file, queue_table)
            VALUES ($1, $2);""",
            os.path.basename(migrate_file),
            f"{queue_table_schema}.{queue_table_name}",
        )


async def create_table(queue_table_schema: str, queue_table_name: str) -> None:
    print(f"/* Creating table {queue_table_schema}.{queue_table_name}... */")
    async with DBConnection() as cnx:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        schma_path = os.path.join(os.path.dirname(dir_path), "schema")
        migrate_files = [
            (os.path.join(schma_path, filepath))
            for filepath in os.listdir(schma_path)
            if filepath.endswith(".sql")
        ]
        migrate_files.sort()
        async with cnx.transaction():
            for migrate_file in migrate_files:
                if await is_migration_executed(
                    cnx,
                    os.path.basename(migrate_file),
                    queue_table_schema,
                    queue_table_name,
                ):
                    continue
                else:
                    await migrate(
                        cnx, migrate_file, queue_table_schema, queue_table_name
                    )
    print(f"/* Done */")


async def wait_for_notify(
    queue_table_schema: str, queue_table_name: str, q: asyncio.Queue
):
    """ Wait for notification and put channel to the Queue """
    print(f"/* LISTENING ON {queue_table_schema}_{queue_table_name}... */")
    async with DBConnection() as cnx:

        def notified(cnx: Connection, pid: int, channel: str, payload: str):
            print("/* Notified, shutting down... */")
            asyncio.gather(cnx.close(), q.put(channel))

        await cnx.add_listener(f"{queue_table_schema}_{queue_table_name}", notified)


async def shutdown(q: asyncio.Queue):
    """ Gracefully shutdown when something put to the Queue """
    await q.get()
    sys.exit()


async def delete_executed_job(
    queue_table: str,
    cnx: asyncpg.Connection = None,
    repeat: bool = False,
    scheduled_day: int = 1,
    keep_period_day: int = 0,
    batch_size: int = 0,
) -> str:
    """ Delete executed Job """
    if not cnx:
        async with DBConnection() as cnx:
            async with cnx.transaction():
                return await exec_delete_executed_job(
                    cnx,
                    queue_table,
                    repeat=repeat,
                    scheduled_day=scheduled_day,
                    keep_period_day=keep_period_day,
                    batch_size=batch_size,
                )
    else:
        return await exec_delete_executed_job(
            cnx,
            queue_table,
            repeat=repeat,
            scheduled_day=scheduled_day,
            keep_period_day=keep_period_day,
            batch_size=batch_size,
        )


async def exec_delete_executed_job(
    cnx: asyncpg.Connection,
    queue_table: str,
    repeat: bool = False,
    scheduled_day: int = 1,
    keep_period_day: int = 0,
    batch_size: int = 0,
) -> None:
    sql = """DELETE FROM {queue_table} WHERE id IN (
        SELECT id FROM {queue_table} WHERE executed IS NOT NULL
        AND executed < NOW() - INTERVAL '{keep_period_day} days' ORDER BY executed );""".format(
        queue_table=sql_table_name(queue_table), keep_period_day=keep_period_day
    )

    executed_job = 0
    if batch_size:
        sql = sql.replace(");", f"LIMIT {batch_size});")
        executed_job = await cnx.fetchval(
            """
            SELECT COUNT(*) - $1 FROM {} WHERE executed IS NOT NULL
            AND executed < NOW() - INTERVAL  '{} days'
        """.format(
                sql_table_name(queue_table), keep_period_day
            ),
            batch_size,
        )

    deleted = await cnx.execute(sql)
    if executed_job > 0:
        kwargs = {
            "repeat": repeat,
            "scheduled_day": scheduled_day,
            "keep_period_day": keep_period_day,
            "batch_size": batch_size,
        }
        await cnx.execute(
            """INSERT INTO {} (id, fn_name, args, kwargs)
            VALUES ($1, 'mod_ngarn.utils.delete_executed_job', $2, $3)
        """.format(
                sql_table_name(queue_table)
            ),
            str(uuid.uuid4()),
            queue_table,
            kwargs,
        )
    elif repeat:
        next_scheduled = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(
            days=scheduled_day
        )
        kwargs = {
            "repeat": repeat,
            "scheduled_day": scheduled_day,
            "keep_period_day": keep_period_day,
            "batch_size": batch_size,
        }
        await cnx.execute(
            """INSERT INTO {} (id, fn_name, args, kwargs, scheduled)
            VALUES ($1, 'mod_ngarn.utils.delete_executed_job', $2, $3, $4)
        """.format(
                sql_table_name(queue_table)
            ),
            str(uuid.uuid4()),
            queue_table,
            kwargs,
            next_scheduled,
        )

    return deleted
