import functools
from typing import Any, Callable, Optional, Protocol, Iterable
import threading
import time
import logging

class FallbackFailure(Exception):
    """
    表示所有fallback方案都已失败
    """


def fallback(tolerable_exceptions: Optional[Iterable] = None):
    """
    装饰函数之后，可以使用被装饰函数的`register`装饰器方法注册来用于fallback的函数，
    要求fallback函数的参数与被装饰函数的一致。
    """

    def decorator(func):
        return Fallbacker(func, tolerable_exceptions=tolerable_exceptions)

    return decorator


class Fallbacker:
    """
    用于做fallback装饰的装饰器类
    （不完全是，需要fallback函数做辅助）
    """

    def __init__(self, func: Callable, tolerable_exceptions: Optional[Iterable] = None):
        self._tol_excs = (
            tuple(tolerable_exceptions) if tolerable_exceptions else (Exception,)
        )
        self._func = func
        self._fallback_funcs: list[Callable] = []

    def __call__(self, *args, **kwargs) -> Any:
        try:
            return self._func(*args, **kwargs)
        except self._tol_excs:
            if self._fallback_funcs:
                return self._do_fallback(*args, **kwargs)
            raise

    def _do_fallback(self, *args, **kwargs):
        for func in self._fallback_funcs:
            try:
                return func(*args, **kwargs)
            except self._tol_excs as e:
                logging.warning("Tolerated exception while falling back: %s", e)
        raise FallbackFailure("No more func to fallback")

    def register(self, func: Callable):
        """注册一个函数用于fallback"""
        self._fallback_funcs.append(func)
        return func


def decorate(func: Callable, *decorators: Callable):
    """
    对第一个参数函数，
    用后面紧跟的所有装饰器函数依序装饰它，返回装饰完成的函数
    """
    for deco in decorators:
        func = deco(func)
    return func


def discard_return(func: Callable[..., Any]) -> Callable[..., None]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)

    return wrapper

class _DecoratorType(Protocol):
    def __call__(self, func: Callable) -> Callable: ...


def compress(*decos: _DecoratorType):
    '''将多个装饰器「压缩」成一个'''
    def deco(func: Callable):
        f = func
        for deco in reversed(decos):
            f = deco(f)
        return f

    return deco


def only_one_running(func):
    '''总之就是加锁'''
    return functools.wraps(func)(OnlyOneRunning(func))


class OnlyOneRunning:
    def __init__(self, func) -> None:
        self._func = func
        self._lock = threading.Lock()

    def __call__(self, *args, **kwargs):
        with self._lock:
            return self._func(*args, **kwargs)


def cache(expire: float | int = 60):
    def deco(func):
        return functools.wraps(func)(CacheWrapper(func, expire=expire))

    return deco


class CacheWrapper:
    def __init__(self, func, expire: float | int = 60) -> None:
        self._func = func
        self._expire = expire
        self._last_call: int | float = 0
        self._cache: Optional[Any] = None
        self._lock = threading.Lock()

    def __call__(self, *args, **kwargs):
        with self._lock:
            if self._cache is None or time.time() - self._last_call >= self._expire:
                self._cache = self._func(*args, **kwargs)
                self._last_call = time.time()
        return self._cache


def schedule(interval: float | int):
    def deco(func):
        return functools.wraps(func)(Scheduler(func, interval=interval))

    return deco


class Scheduler:
    def __init__(self, func: Callable[[], Any], interval: float | int) -> None:
        self._interval = interval
        self._func = func
        self._thread = threading.Thread(target=func, daemon=True)

    def start(self):
        return self._thread.start()

    def _schedule_worker(self):
        while True:
            time.sleep(self._interval)
            self._func()

    def __call__(self):
        return self._func()
