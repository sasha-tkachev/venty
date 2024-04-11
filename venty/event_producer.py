import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, TypeVar
from uuid import UUID, uuid4, uuid5

from cloudevents.pydantic import CloudEvent
from cloudevents.sdk.event.attribute import (
    default_id_selection_algorithm,
    default_time_selection_algorithm,
)
from pydantic import BaseModel

from venty.strong_types import EventSource


def _ignore_invalid_attributes(attributes: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: v for k, v in attributes.items() if k not in {"data", "id", "source", "time"}
    }


IdSelection = Callable[[], str]
TimeSelection = Callable[[], datetime]


class EventProducer:
    def produce_event(
        self, attributes: Dict[str, Any], data: Optional[Any]
    ) -> CloudEvent:
        raise NotImplementedError()


def produce_simple_event(
    type_: str,
    *,
    attributes: Optional[Dict[str, Any]] = None,
    data: Optional[Any] = None,
    event_producer: EventProducer,
) -> CloudEvent:
    if attributes is None:
        attributes = {}
    attributes = attributes.copy()  # prevent mutation
    attributes["type"] = type_
    if isinstance(data, BaseModel):
        data = data.json(exclude_none=True)
    return event_producer.produce_event(attributes, data)


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
        self, attributes: Dict[str, Any], data: Optional[Any]
    ) -> CloudEvent:
        actual_attributes = self._default_attributes.copy()
        actual_attributes.update(attributes)
        actual_attributes["source"] = self._source
        actual_attributes["id"] = self._id_selection_algorithm()
        actual_attributes["time"] = self._time_selection_algorithm()
        return CloudEvent.create(actual_attributes, data)


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
