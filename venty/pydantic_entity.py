from typing import List

from cloudevents.abstract import CloudEvent

try:
    from pydantic import BaseModel, PrivateAttr
except ImportError:  # pragma: no cover # hard to test
    raise RuntimeError(
        "Venty pydantic feature is not installed. "
        "Install it using pip install venty[pydantic]"
    )


from venty.entity import Entity as _Entity, EntityId
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

    @property
    def entity_id(self) -> EntityId:
        raise NotImplementedError()

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()
