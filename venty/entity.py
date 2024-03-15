from dataclasses import dataclass, field
from typing import List, NewType, Iterable, TypeVar

from cloudevents.abstract import CloudEvent

from venty.strong_types import StreamVersion, NO_EVENT_VERSION

EntityId = NewType("EntityId", str)


class Entity:
    @property
    def entity_version(self) -> StreamVersion:
        raise NotImplementedError()

    def set_entity_version(self, version: StreamVersion) -> None:
        raise NotImplementedError()

    @property
    def entity_changes(self) -> List[CloudEvent]:
        raise NotImplementedError()

    def clear_entity_changes(self) -> None:
        raise NotImplementedError()

    @property
    def entity_id(self) -> EntityId:
        raise NotImplementedError()

    @classmethod
    def entity_type(cls) -> str:
        return cls.__name__

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()

    def apply(self, event: CloudEvent) -> None:
        self.entity_changes.append(event)
        self.when(event)

    def load(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            self.when(event)
            self.set_entity_version(StreamVersion(self.entity_version + 1))


@dataclass
class DataclassEntity(Entity):
    __version: StreamVersion = NO_EVENT_VERSION
    __changes: List[CloudEvent] = field(default_factory=list)

    @property
    def entity_version(self) -> StreamVersion:
        return self.__version

    def set_entity_version(self, version: StreamVersion) -> None:
        self.__version = version

    @property
    def entity_changes(self) -> List[CloudEvent]:
        return self.__changes

    def clear_entity_changes(self) -> None:
        self.__changes = []

    @property
    def entity_id(self) -> EntityId:
        raise NotImplementedError()

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()


EntityT = TypeVar("EntityT", bound=Entity)
