import pytest

from mod_ngarn.utils import sql_table_name


@pytest.mark.asyncio
async def test_sql_table_name_should_return_quote_string():
    assert '"a/b"."test/table"' == sql_table_name('a/b.test/table')
    assert '"public"."mod_ngarn_job"' == sql_table_name('public.mod_ngarn_job')
    assert '"mod_ngarn_job"' == sql_table_name('mod_ngarn_job')