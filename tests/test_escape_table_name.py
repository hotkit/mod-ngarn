import pytest

from mod_ngarn.utils import escape_table_name


@pytest.mark.asyncio
async def test_escape_table_name_should_return_fist_string():
    assert 'TEST' == escape_table_name('TEST    A B')
    assert 'TEST' == escape_table_name('TEST,AB')
    assert 'TEST' == escape_table_name('TEST\"B')
    assert 'TEST' == escape_table_name('TEST\'B')
    assert 'TEST' == escape_table_name('TEST INNER JOIN B ON ')
