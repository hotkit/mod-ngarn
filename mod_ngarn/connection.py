import json
import os

import asyncpg


async def get_connection():
    PGDBNAME = os.getenv('PGDBNAME')
    PGHOST = os.getenv('PGHOST')
    PGPASSWORD = os.getenv('PGPASSWORD')
    PGUSER = os.getenv('PGUSER')
    cnx = await asyncpg.connect(user=PGUSER, password=PGPASSWORD, database=PGDBNAME, host=PGHOST)
    await cnx.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')
    await cnx.set_type_codec('json', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')
    return cnx
