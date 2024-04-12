from typing import Iterable

from cloudevents.abstract import CloudEvent

from venty.aggregate_root import AggregateRoot
from venty.event_channel import EventChannel


class AggregateChannel(EventChannel):
    def __init__(self, aggregate: AggregateRoot):
        self._aggregate = aggregate

    def publish(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            self._aggregate.apply(event)
