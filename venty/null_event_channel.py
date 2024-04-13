from typing import Iterable

from cloudevents.abstract import CloudEvent

from venty.event_channel import EventChannel


class NullEventChannel(EventChannel):
    def publish(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            pass
