from typing import Callable, Union
from inspect import getmembers, getmodule, ismethod


class ImportNotFoundException(Exception):
    pass


class ModuleNotfoundException(Exception):
    pass


async def get_fn_name(func: Union[str, Callable]) -> str:
    try:
        if isinstance(func, str):
            return func
        if ismethod(func):
            module_name = get_fn_name(dict(getmembers(func))['__self__'])
        else:
            module_name = getmodule(func).__name__
        name = func.__name__
        return '.'.join([module_name, name])
    except AttributeError as e:
        raise ModuleNotfoundException(e)


async def import_fn(fn_name) -> Callable:
    access_path = fn_name.split(".")
    module = None
    try:
        for index in range(1, len(access_path)):
            try:
                # import top level module
                module_name = ".".join(access_path[:-index])
                module = __import__(module_name)
            except ImportError:
                continue
            else:
                for step in access_path[1:-1]:  # walk down it
                    module = getattr(module, step)
                break
        if module:
            return getattr(module, access_path[-1])
        else:
            return globals()["__builtins__"][fn_name]
    except KeyError as e:
        raise ImportNotFoundException(e)
