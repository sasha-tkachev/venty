from dataclasses import dataclass, field
from typing import List, NewType, Iterable, TypeVar
from uuid import UUID

from cloudevents.abstract import CloudEvent

from venty.strong_types import StreamVersion, NO_EVENT_VERSION


AggregateUUID = NewType("AggregateUUID", UUID)


@dataclass
class AggregateRoot:
    """
    Based on https://github.com/gregoryyoung/m-r/blob/master/SimpleCQRS/Domain.cs#L89
    """

    _aggregate_version: StreamVersion = NO_EVENT_VERSION
    _uncommitted_changes: List[CloudEvent] = field(default_factory=list)

    def __init__(self):
        super().__init__()

    @property
    def aggregate_version(self) -> StreamVersion:
        """
        not called "version" to avoid name collision with subclasses
        """
        return self._aggregate_version

    @property
    def uncommitted_changes(self) -> List[CloudEvent]:
        return self._uncommitted_changes

    def mark_changes_as_committed(self) -> None:
        self._uncommitted_changes.clear()

    @property
    def aggregate_uuid(self) -> AggregateUUID:
        """
        If you have a string aggregate id, you can convert it to uuid using uuidv5
        """
        raise NotImplementedError()

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()

    def apply(self, event: CloudEvent) -> None:
        self.uncommitted_changes.append(event)
        self.when(event)

    def load_from_history(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            self.when(event)
            self._aggregate_version = StreamVersion(self.aggregate_version + 1)


AggregateRootT = TypeVar("AggregateRootT", bound=AggregateRoot)
