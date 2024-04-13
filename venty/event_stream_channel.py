from typing import Iterable

from venty import EventStore, append_events
from venty.cloudevent import CloudEvent
from venty.event_channel import EventChannel
from venty.event_store import StreamState
from venty.strong_types import StreamName


class EventStreamChannel(EventChannel):
    def __init__(self, event_store: EventStore, stream_name: StreamName):
        self._event_store = event_store
        self._stream_name = stream_name

    def publish(self, events: Iterable[CloudEvent]) -> None:
        append_events(
            event_store=self._event_store,
            stream_name=self._stream_name,
            expected_version=StreamState.ANY,
            events=events,
        )
