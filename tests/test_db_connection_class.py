import os

import pytest 
from mod_ngarn.connection import DBConnection, get_connection


@pytest.mark.asyncio
async def test_db_connection_class_should_work():
    async with DBConnection() as connection:
        await connection.execute("""
            DROP TABLE IF EXISTS test_table CASCADE;
            CREATE TABLE test_table (name TEXT PRIMARY KEY);
            INSERT INTO test_table VALUES('01');
        """)

    cnx = await get_connection()
    result = await cnx.fetchval('SELECT  * FROM test_table')
    assert result == '01'