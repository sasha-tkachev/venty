from typing import Dict, Any, Optional

from cloudevents.pydantic import CloudEvent

from venty.event_producer import EventProducer
from collections import OrderedDict
from uuid import uuid4, UUID


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
        self, attributes: Dict[str, Any], data: Optional[Any]
    ) -> CloudEvent:
        return _last(self._event_producers).produce_event(attributes, data)

    def scoped_event_producer(self, event_producer: EventProducer) -> None:
        scoped_uuid = uuid4()
        self._event_producers[scoped_uuid] = event_producer
        try:
            yield event_producer
        finally:
            self._event_producers.pop(scoped_uuid)
