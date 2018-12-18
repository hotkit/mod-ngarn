import pytest

from mod_ngarn.utils import get_fn_name, import_fn, ModuleNotfoundException


# TEST functions
async def async_sum(first, second):
    import asyncio

    await asyncio.sleep(1)
    return first + second


def sync_sum(first, second):
    return first + second


@pytest.mark.asyncio
async def test_get_fn_name_with_async_callable_should_return_string():
    res = await get_fn_name(async_sum)
    assert res == 'tests.test_get_fn_name.async_sum'


@pytest.mark.asyncio
async def test_get_fn_name_with_sync_callable_should_return_string():
    res = await get_fn_name(sync_sum)
    assert res == 'tests.test_get_fn_name.sync_sum'


@pytest.mark.asyncio
async def test_get_fn_name_with_string_should_return_string_same_as_input():
    res = await get_fn_name('sum_func')
    assert res == 'sum_func'


@pytest.mark.asyncio
async def test_generated_fn_name_should_return_result_after_transform_back_to_callable():
    fn_name = await get_fn_name(async_sum)
    func = await import_fn(fn_name)

    actual = await func(11, 12)
    expected = await async_sum(11, 12)

    assert expected == actual


@pytest.mark.asyncio
async def test_not_get_fn_name_with_non_module_or_callable_should_raise_exception():
    func = object()
    with pytest.raises(ModuleNotfoundException):
        await get_fn_name(func)
