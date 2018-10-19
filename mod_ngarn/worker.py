import asyncio
import functools
import logging
import sys
import time
import traceback
from decimal import Decimal
import os

import asyncpg

from mod_ngarn.connection import get_connection

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger('mod_ngarn')

LIMIT = os.getenv('LIMIT', 10)

async def fetch_job(cnx: asyncpg.Connection):
    return await cnx.fetchrow("""
        SELECT id, fn_name, args, kwargs, priority
        FROM modngarn_job 
        WHERE executed IS NULL
            AND (scheduled IS NULL OR scheduled < NOW())
            AND priority <= $1
            AND canceled IS NULL
        ORDER BY priority 
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    """, LIMIT)


async def import_fn(function_name: str):
    access_path = function_name.split('.')
    module = None
    for index in range(1, len(access_path)):
        try:
            # import top level module
            module_name = '.'.join(access_path[:-index])
            module = __import__(module_name)
        except ImportError:
            continue
        else:
            for step in access_path[1:-1]: # walk down it
                module = getattr(module, step)
            break
    if module:
        return getattr(module, access_path[-1])
    else:
        return globals()['__builtins__'][function_name]


async def execute(job: asyncpg.Record):
    """ Execute the transaction """
    func = await import_fn(job['fn_name'])
    if asyncio.iscoroutinefunction(func):
        return await func(*job['args'], **job['kwargs'])
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *job['args'], **job['kwargs']))


async def record_success_result(cnx: asyncpg.Connection, job: asyncpg.Record, processed_time: str, result: dict ={}):
    """ Record result to database, does not commit the transaction """
    return await cnx.execute("""
        UPDATE modngarn_job SET result=$1, executed=NOW(), processed_time=$2 WHERE id=$3
    """, result, processed_time, job['id'])


async def record_failed_result(cnx: asyncpg.Connection, job: asyncpg.Record, error: str):
    """ Record result to database, does not commit the transaction """
    if job['priority'] == LIMIT:    
        return await cnx.execute("""
            UPDATE modngarn_job SET priority=priority+1, reason=$2, canceled=NOW() WHERE id=$1
        """, job['id'], error)
    return await cnx.execute("""
        UPDATE modngarn_job SET priority=priority+1 WHERE id=$1
    """, job['id'])

async def run():
    cnx = await get_connection()
    async with cnx.transaction():
        job = await fetch_job(cnx)
        if job:
            try:
                start_time = time.time()
                result = await execute(job)
                processed_time = str(Decimal(str(time.time() - start_time)).quantize(Decimal('.001')))
                log.info('Processed#{}  in {}s'.format(job['id'], processed_time))
                await record_success_result(cnx, job, processed_time, result=result)
            # TODO: More specific Exception
            except Exception as e:
                log.error('Error#{}, {}'.format(job['id'], e.__repr__()))
                log.error(traceback.print_exc())
                await record_failed_result(cnx, job, e.__repr__())
    await cnx.close()


def run_worker():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

if __name__ == '__main__':
    run_worker()
