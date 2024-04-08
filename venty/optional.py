from typing import TypeVar, Optional, Callable

T = TypeVar("T")


def not_none(
    value: Optional[T],
    raise_on_none: Callable[[], Exception] = lambda: ValueError(
        "Value must not be None"
    ),
) -> T:
    if value is None:
        raise raise_on_none()
    return value
