from typing import Dict, Any, Optional, ContextManager, Type

from cloudevents.pydantic import CloudEvent

from venty.event_producer import (
    EventProducer,
    EventProducerT,
    AttributeValue,
)
from venty.strong_types import CloudEventT
from collections import OrderedDict
from uuid import uuid4, UUID
from contextlib import contextmanager
import sys

if sys.version_info < (3, 9):
    from typing import OrderedDict as OrderedDictType

    EventProducers = OrderedDictType[UUID, Optional[EventProducer]]
else:
    EventProducers = OrderedDict[UUID, Optional[EventProducer]]


def _last(event_producers: EventProducers) -> EventProducer:
    # it is O(1)
    # https://stackoverflow.com/questions/9917178/last-element-in-ordereddict
    _, last_value = next(reversed(event_producers.items()))
    return last_value


class EventProducerStack(EventProducer):
    def __init__(self, default_event_producer: EventProducer):
        self._event_producers: EventProducers = OrderedDict()
        self._event_producers[uuid4()] = default_event_producer

    def produce_event(
        self,
        type_: Type[CloudEventT],
        data: Optional[Any],
        *,
        attributes: Optional[Dict[str, AttributeValue]] = None,
    ) -> CloudEventT:
        return _last(self._event_producers).produce_event(
            type_, data=data, attributes=attributes
        )

    @contextmanager
    def scoped_event_producer(
        self, event_producer: EventProducerT
    ) -> ContextManager[EventProducerT]:
        scoped_uuid = uuid4()
        self._event_producers[scoped_uuid] = event_producer
        try:
            yield event_producer
        finally:
            self._event_producers.pop(scoped_uuid)
