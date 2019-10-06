import asyncio
import asyncpg
import os
import re
import sys
from datetime import datetime, timedelta
from inspect import getmembers, getmodule, ismethod
from typing import Callable, Union

from asyncpg.connection import Connection

from .api import delete_executed_job as api_delete_executed_job
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


async def is_migration_executed(cnx: asyncpg.connection, migrate_file: str, queue_table_schema: str, queue_table_name: str) -> bool:
    migration_table =  await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.mod_ngarn_migration');
        """)
    if not migration_table:
        return False

    return await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM public.mod_ngarn_migration
            WHERE migrate_file = $1 AND queue_table = $2);
        """, migrate_file, f'{queue_table_schema}.{queue_table_name}')


async def migrate(cnx: asyncpg.connection, migrate_file: str, queue_table_schema: str, queue_table_name: str) -> None:
    with open(migrate_file) as f:
        sql = f.read()
        sql = sql.replace("{queue_table_schema}", queue_table_schema)
        sql = sql.replace("{queue_table_name}", queue_table_name)
        await cnx.execute(sql)
        await cnx.execute("""
            INSERT INTO public.mod_ngarn_migration(migrate_file, queue_table)
            VALUES ($1, $2);"""
        ,os.path.basename(migrate_file), f'{queue_table_schema}.{queue_table_name}')


async def create_table(queue_table_schema: str, queue_table_name: str)->None:
    print(f"/* Creating table {queue_table_schema}.{queue_table_name}... */")
    async with DBConnection() as cnx:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        schma_path = os.path.join(os.path.dirname(dir_path), 'schema')
        migrate_files = [(os.path.join(schma_path ,filepath)) for filepath in os.listdir(schma_path) if filepath.endswith(".sql")]
        migrate_files.sort()
        async with cnx.transaction():
            for migrate_file in migrate_files:
                if await is_migration_executed(cnx, os.path.basename(migrate_file), queue_table_schema, queue_table_name):
                    continue
                else:
                    await migrate(cnx, migrate_file, queue_table_schema, queue_table_name)
    print(f"/* Done */")


async def wait_for_notify(queue_table_schema: str, queue_table_name: str, q: asyncio.Queue):
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


async def delete_executed_job(queue_table: str) -> None:
    async with DBConnection() as cnx:
        await api_delete_executed_job(cnx, queue_table)