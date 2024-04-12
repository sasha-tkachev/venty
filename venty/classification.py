from typing import Optional, TypeVar, Any

from cloudevents.pydantic import CloudEvent


T = TypeVar("T")


def may_be(type_: type[T], value: Any) -> Optional[T]:
    if isinstance(value, type_):
        return value
    if issubclass(type_, CloudEvent):
        if value.type == type_.__fields__["type"].default:
            return type_.parse_obj(value.dict())
    return None
