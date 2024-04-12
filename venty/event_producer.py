import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, TypeVar, Type, Union
from uuid import UUID, uuid4, uuid5

from cloudevents.sdk.event.attribute import (
    default_id_selection_algorithm,
    default_time_selection_algorithm,
)

from venty.strong_types import EventSource, CloudEventT


def _ignore_invalid_attributes(attributes: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: v for k, v in attributes.items() if k not in {"data", "id", "source", "time"}
    }


IdSelection = Callable[[], str]
TimeSelection = Callable[[], datetime]

AttributeValue = Union[str, int, UUID, bool]


class EventProducer:
    def produce_event(
        self,
        type_: Type[CloudEventT],
        data: Optional[Any],
        *,
        attributes: Optional[Dict[str, AttributeValue]] = None,
    ) -> CloudEventT:
        raise NotImplementedError()


EventProducerT = TypeVar("EventProducerT", bound=EventProducer)


def deterministic_id_factory(seed: int = 0) -> Callable[[], str]:
    i = iter(range(sys.maxsize))

    def _next() -> str:
        return str(uuid5(UUID(int=seed), str(next(i))))

    return _next


def deterministic_time_factory() -> Callable[[], datetime]:
    i = iter(range(sys.maxsize))

    def _next() -> datetime:
        return datetime.fromtimestamp(next(i), tz=timezone.utc)

    return _next


def _normalize_attributes(
    attributes: Dict[str, AttributeValue]
) -> Dict[str, Union[str, int, bool]]:
    return {
        k: v if isinstance(v, (str, int, bool)) else str(v)
        for k, v in attributes.items()
    }


class SimpleEventProducer(EventProducer):
    def __init__(
        self,
        *,
        source: Optional[EventSource] = None,
        default_attributes: Optional[Dict[str, Any]] = None,
        id_selection_algorithm: IdSelection = default_id_selection_algorithm,
        time_selection_algorithm: TimeSelection = default_time_selection_algorithm,
    ):
        if source is None:
            source = EventSource(str(uuid4()))
        self._source = source
        if default_attributes is None:
            default_attributes = {}
        self._default_attributes = _ignore_invalid_attributes(default_attributes)
        self._id_selection_algorithm = id_selection_algorithm
        self._time_selection_algorithm = time_selection_algorithm

    def produce_event(
        self,
        type_: Type[CloudEventT],
        data: Optional[Any],
        *,
        attributes: Optional[Dict[str, AttributeValue]] = None,
    ) -> CloudEventT:
        if attributes is None:
            attributes = {}
        actual_attributes = self._default_attributes.copy()
        actual_attributes.update(attributes)
        actual_attributes["source"] = self._source
        actual_attributes["id"] = self._id_selection_algorithm()
        actual_attributes["time"] = self._time_selection_algorithm()
        return type_.create(_normalize_attributes(actual_attributes), data)


def testing_event_producer(
    *,
    source: Optional[EventSource] = EventSource("fake-source"),
    default_attributes: Optional[Dict[str, Any]] = None,
    seed: int = 0,
) -> EventProducer:
    return SimpleEventProducer(
        source=source,
        default_attributes=default_attributes,
        id_selection_algorithm=deterministic_id_factory(seed),
        time_selection_algorithm=deterministic_time_factory(),
    )
