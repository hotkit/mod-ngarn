import asyncio
import asyncpg

async def get_connection():
    return await asyncpg.connect(user='tle', password='',
                                 database='mod-ngarn-test', host='127.0.0.1')
    # values = await conn.fetch('''SELECT * FROM mytable''')
    # await conn.close()
