import asyncpg
import os

async def get_connection():
    PGDBNAME = os.getenv('PGDBNAME')
    PGHOST = os.getenv('PGHOST')
    PGPASSWORD = os.getenv('PGPASSWORD')
    PGUSER = os.getenv('PGUSER')
    return await asyncpg.connect(user=PGUSER, password=PGPASSWORD,
        database=PGDBNAME, host=PGHOST)
