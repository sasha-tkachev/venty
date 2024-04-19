from typing import Iterable

from pydantic import BaseModel, PrivateAttr

from venty.cloudevent import CloudEvent
from venty.strong_types import NO_EVENT_VERSION, StreamVersion


class Projection(BaseModel):
    _aggregate_version: StreamVersion = PrivateAttr(NO_EVENT_VERSION)

    def aggregate_version(self) -> StreamVersion:
        """
        not called "version" to avoid name collision with subclasses
        """
        return self._aggregate_version

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()

    def load_from_history(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            self.when(event)
            self._aggregate_version = StreamVersion(self.aggregate_version() + 1)
