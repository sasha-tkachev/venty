from typing import Optional, TypeVar, Any, Type

from cloudevents.pydantic import CloudEvent


T = TypeVar("T")


def may_be(type_: Type[T], value: Any) -> Optional[T]:
    if isinstance(value, type_):
        return value
    if issubclass(type_, CloudEvent):
        if value.type == type_.__fields__["type"].default:
            return type_.parse_obj(value.dict())
    return None


def must_be(type_: Type[T], value: Any) -> T:
    result = may_be(type_, value)
    if result is None:
        raise ValueError(f"Expected {type_.__name__}, got {value.__class__.__name__}")
    return result


def must_be_list_of(type_: Type[T], value: Any) -> list[T]:
    return [must_be(type_, item) for item in must_be(list, value)]
