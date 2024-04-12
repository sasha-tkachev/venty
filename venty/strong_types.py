from typing import NewType, TypeVar

from venty.cloudevent import CloudEvent

StreamName = NewType("StreamName", str)
StreamVersion = NewType("StreamVersion", int)
NO_EVENT_VERSION = StreamVersion(-1)
CommitPosition = NewType("CommitPosition", int)
EventType = NewType("EventType", str)
EventSource = NewType("EventSource", str)
CloudEventT = TypeVar("CloudEventT", bound=CloudEvent)
