import os
import re
from inspect import getmembers, getmodule, ismethod
from typing import Callable, Union

from .connection import get_connection


class ImportNotFoundException(Exception):
    pass


class ModuleNotfoundException(Exception):
    pass


def sql_table_name(queue_table: str) -> str:
    return ('.').join([f'"{x}"' for x in queue_table.split('.')])   


async def get_fn_name(func: Union[str, Callable]) -> str:
    try:
        if isinstance(func, str):
            return func
        if ismethod(func):
            module_name = get_fn_name(dict(getmembers(func))['__self__'])
        else:
            module_name = getmodule(func).__name__
        name = func.__name__
        return '.'.join([module_name, name])
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


async def create_table(name: str):
    if not name:
        name = os.getenv('MOD_NGARN_TABLE', 'modngarn_job')
    print(f"Creating table {name}...")
    cnx = await get_connection()
    async with cnx.transaction():
        await cnx.execute(
            """CREATE TABLE IF NOT EXISTS {queue_table} (
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
    print(f"Done")
