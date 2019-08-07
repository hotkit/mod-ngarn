import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import is_table_exists


@pytest.mark.asyncio
async def test_is_table_exists_can_check_existing_of_table_id_db():
    cnx = await get_connection()
    await cnx.execute("CREATE TABLE public.test_init_table (id TEXT);")

    assert True == await is_table_exists("public.test_init_table")
    assert True == await is_table_exists('"public"."test_init_table"')
    assert False == await is_table_exists("test_init_table")
    assert False == await is_table_exists('"test_init_table"')

    assert False == await is_table_exists("public.f_test_init_table")
    assert False == await is_table_exists('"public"."f_test_init_table"')
    assert False == await is_table_exists("f_test_init_table")
    assert False == await is_table_exists('"f_test_init_table"')

    await cnx.execute("DROP TABLE public.test_init_table;")
    await cnx.close()
