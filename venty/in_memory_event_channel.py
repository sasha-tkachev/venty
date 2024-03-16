from typing import Iterable, Tuple

from cloudevents.abstract import CloudEvent

from venty.event_channel import EventChannel


class InMemoryEventChannel(EventChannel):
    """
    Use this for testing purposes.
    """

    def __init__(self):
        self._published_events = []

    def publish(self, events: Iterable[CloudEvent]) -> None:
        self._published_events.extend(events)

    @property
    def published_events(self) -> Tuple[CloudEvent, ...]:
        """
        Tuple to prevent internal list mutation
        """
        return tuple(self._published_events)
