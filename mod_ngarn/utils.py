import asyncio
import asyncpg
import os
import re
import sys
from datetime import datetime, timedelta
from inspect import getmembers, getmodule, ismethod
from typing import Callable, Union

from asyncpg.connection import Connection

from .connection import get_connection


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


def notify_channel(queue_table: str) -> str:
    return queue_table.replace('"', "").replace(".", "_")


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


async def is_table_exists(name: str) -> bool:
    cnx = await get_connection()
    return await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables 
            WHERE schemaname || '.' || tablename = '{queue_table}');
        """.format(
            queue_table=name.replace('"', "")
        )
    )


async def exec_create_table(cnx: asyncpg.connection, name: str):
    await cnx.execute(
        """CREATE TABLE {queue_table} (
                id TEXT NOT NULL CHECK (id !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c'),
                fn_name TEXT NOT NULL,
                args JSON DEFAULT '[]',
                kwargs JSON DEFAULT '{{}}',
                priority INTEGER DEFAULT 0,
                created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                scheduled TIMESTAMP WITH TIME ZONE,
                executed TIMESTAMP WITH TIME ZONE,
                canceled TIMESTAMP WITH TIME ZONE,
                result JSON,
                reason TEXT,
                processed_time TEXT,
                PRIMARY KEY (id)
            );
        """.format(
            queue_table=name
        )
    )

    await cnx.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_pending_jobs ON {name} (executed) WHERE executed IS NULL;"""
    )

    await cnx.execute(
        """
    CREATE OR REPLACE FUNCTION {notify_channel}_notify_job()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        NOTIFY {notify_channel};
        RETURN NEW;
    END;
    $$;

    DROP TRIGGER IF EXISTS {notify_channel}_notify_job_inserted ON {table_name};
    CREATE TRIGGER {notify_channel}_notify_job_inserted
    AFTER INSERT ON {table_name}
    FOR EACH ROW
    EXECUTE PROCEDURE {notify_channel}_notify_job();
    """.format(
            notify_channel=notify_channel(name), table_name=name
        )
    )


async def exec_create_log_table(cnx: asyncpg.connection, name: str):
    await cnx.execute(
        """CREATE TABLE IF NOT EXISTS {queue_table} (
                id TEXT NOT NULL CHECK (id !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c'),
                fn_name TEXT NOT NULL,
                args JSON DEFAULT '[]',
                kwargs JSON DEFAULT '{{}}',
                message TEXT NOT NULL,
                posted TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                processed_time TEXT,
                PRIMARY KEY (id, posted)
            );
        """.format(
            queue_table=name
        )
    )


async def create_table(name: str):
    print(f"Creating table {name}...")
    cnx = await get_connection()
    async with cnx.transaction():
        if await is_table_exists(name):
            print(
                "Table {queue_table} already exists, skipped".format(queue_table=name)
            )
        else:
            await exec_create_table(cnx, name)

        log_table_name = sql_table_name(name.replace('"', "") + "_error")
        if await is_table_exists(log_table_name):
            print(
                "Table {queue_table} already exists, skipped".format(
                    queue_table=log_table_name
                )
            )
        else:
            await exec_create_log_table(cnx, log_table_name)

    print(f"Done")


async def wait_for_notify(queue_table: str, q: asyncio.Queue):
    """ Wait for notification and put channel to the Queue """
    notify_ch = notify_channel(queue_table)
    print(f"LISTENING ON {notify_ch}...")
    cnx = await get_connection()

    def notified(cnx: Connection, pid: int, channel: str, payload: str):
        print("Notified, shutting down...")
        asyncio.gather(cnx.close(), q.put(channel))

    await cnx.add_listener(notify_ch, notified)


async def shutdown(q: asyncio.Queue):
    """ Gracefully shutdown when something put to the Queue """
    await q.get()
    sys.exit()


async def delete_executed_job(queue_table: str) -> str:
    """ Delete executed Job """
    cnx = await get_connection()
    return await cnx.execute(
        """DELETE from {queue_table} where executed is not null""".format(
            queue_table=queue_table
        )
    )
