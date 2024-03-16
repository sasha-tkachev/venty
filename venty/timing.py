from datetime import timedelta
from typing import Iterable, TypeVar, Optional
from datetime import datetime

T = TypeVar("T")


def _time_since(time: datetime) -> timedelta:
    return datetime.now() - time


def _timeout_reached(start: datetime, timeout: timedelta) -> bool:
    return _time_since(start) > timeout


def iterate_with_timeout(
    iterable: Iterable[T],
    timeout: Optional[timedelta] = None,
) -> Iterable[T]:
    if timeout is None:
        for i in iterable:
            yield i
    else:
        start = datetime.now()
        for i in iterable:
            if _timeout_reached(start, timeout):
                raise TimeoutError()
            yield i


def assert_timeout_not_supported(timeout: Optional[timedelta]):
    assert timeout is None, (
        "Timeout is not supported for this function, please submit a feature "
        "request at https://github.com/sasha-tkachev/venty/issues"
    )
