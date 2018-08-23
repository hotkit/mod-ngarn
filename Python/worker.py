import asyncio
import asyncpg


async def fetch_jobs(cnx: asyncpg.Connection, limit:int=10):
    return await cnx.fetch("""
        SELECT id, fn_name, args, kwargs
        FROM modngarn_job 
        WHERE executed IS NULL
          AND (scheduled IS NULL OR scheduled < NOW())
        ORDER BY priority LIMIT $1;
    """, limit)


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
                # import pdb; pdb.set_trace()
                module = getattr(module, step)
            break
    if module:
        return getattr(module, access_path[-1])
    else:
        return globals()['__builtins__'][function_name]


async def execute(cnx: asyncpg.Connection, job: asyncpg.Record):
    """ Execute the transaction """
    func = await import_fn(job['fn_name'])
    if asyncio.iscoroutinefunction(func):
        result = await func(*job['args'], **job['kwargs'])
    else:
        result = await loop.run_in_executor(None, func, *job['args'], **job['kwargs'])
    await record_result(cnx, job['id'], result)


async def record_result(cnx: asyncpg.Connection, job_id: str, result: dict ={}):
    """ Record result to database, does not commit the transaction """
    cnx.execute("""
        UPDATE modngarn_job SET result=$1, executed=NOW() WHERE id=$2
    """, result, job_id)


async def main(cnx: asyncpg.Connection, loop):
    for job in await fetch_jobs(cnx, limit=10):
        await execute(cnx, job)
