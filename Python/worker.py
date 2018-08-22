import asyncio

# Worker
# 1. Fetch job from database
# 2. Import function that define in job name
# 3. Execute it
# 4. Record result to database

async def fetch_jobs(connection, limit=100):
    # Fetch job from database
    jobs = await connection.fetch(
        'SELECT * FROM modngarn_job WHERE executed IS NULL ORDER BY priority DESC LIMIT $1;', limit)
    return jobs
    

async def import_fn(function_name):
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
        fn = getattr(module, access_path[-1])
    else:
        fn = globals()['__builtins__'][function_name]
    return fn, asyncio.iscoroutinefunction(fn)


async def execute(fn, args, kwargs):
    pass
