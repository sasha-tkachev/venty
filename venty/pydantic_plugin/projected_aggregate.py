from typing import List

from cloudevents.abstract import CloudEvent
from pydantic import BaseModel, PrivateAttr

from venty.entity import Entity as _Entity
from venty.strong_types import StreamVersion, NO_EVENT_VERSION


class Entity(BaseModel, _Entity):
    _changes: List[CloudEvent] = PrivateAttr(default_factory=list)
    version: StreamVersion = NO_EVENT_VERSION

    @property
    def entity_changes(self) -> List[CloudEvent]:
        return self._changes

    def clear_entity_changes(self) -> None:
        self._changes = []

    @property
    def entity_version(self) -> StreamVersion:
        return self.version

    def set_entity_version(self, version: StreamVersion) -> None:
        self.version = version

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()

    def apply(self, event: CloudEvent) -> None:
        raise NotImplementedError()
