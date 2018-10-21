import pytest

from mod_ngarn.utils import import_fn, ImportNotFoundException


# TEST functions
async def async_ret(text):
    import asyncio

    await asyncio.sleep(1)
    return text


def sync_ret(text):
    return text


@pytest.mark.asyncio
async def test_import_fn_can_import_builtin_fn():
    s = await import_fn("sum")
    res = s([1, 2, 3])
    assert res == 6


@pytest.mark.asyncio
async def test_import_fn_can_import_sync_fn():
    r = await import_fn("tests.test_import_fn.sync_ret")
    res = r('Hello')
    assert res == "Hello"


@pytest.mark.asyncio
async def test_import_fn_can_import_async_fn():
    r = await import_fn("tests.test_import_fn.async_ret")
    res = await r('Hello')
    assert res == "Hello"


@pytest.mark.asyncio
async def test_import_fn_should_raise_key_error_when_cannot_import():
    with pytest.raises(ImportNotFoundException):
        await import_fn("very_random.fn")
