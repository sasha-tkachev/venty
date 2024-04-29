from typing import List, NewType, Iterable, TypeVar
from uuid import UUID

from cloudevents.abstract import CloudEvent
from pydantic import BaseModel, PrivateAttr

from venty.strong_types import StreamVersion, NO_EVENT_VERSION


AggregateUUID = NewType("AggregateUUID", UUID)


class AggregateRoot(BaseModel):
    """
    Based on https://github.com/gregoryyoung/m-r/blob/master/SimpleCQRS/Domain.cs#L89
    """

    _aggregate_version: StreamVersion = PrivateAttr(NO_EVENT_VERSION)
    _uncommitted_changes: List[CloudEvent] = PrivateAttr(default_factory=list)

    def aggregate_version(self) -> StreamVersion:
        """
        not called "version" to avoid name collision with subclasses
        """
        return self._aggregate_version

    def uncommitted_changes(self) -> List[CloudEvent]:
        return self._uncommitted_changes

    def mark_changes_as_committed(self) -> None:
        self._uncommitted_changes.clear()

    def aggregate_uuid(self) -> AggregateUUID:
        """
        If you have a string aggregate id, you can convert it to uuid using uuidv5
        """
        raise NotImplementedError()

    def when(self, event: CloudEvent) -> None:
        raise NotImplementedError()

    def apply(self, event: CloudEvent) -> None:
        self.when(event)
        self.uncommitted_changes().append(event)

    def load_from_history(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            self.when(event)
            self._aggregate_version = StreamVersion(self.aggregate_version() + 1)


AggregateRootT = TypeVar("AggregateRootT", bound=AggregateRoot)


def subject_aggregate(event: CloudEvent) -> AggregateUUID:
    if subject := event.get("subject"):
        return AggregateUUID(UUID(subject))
    raise ValueError("venty.MissingSubject")


def apply_events(
    events: Iterable[CloudEvent], aggregate: AggregateRootT
) -> AggregateRootT:
    for events in events:
        aggregate.apply(events)
    return aggregate


def apply_event(event: CloudEvent, aggregate: AggregateRootT) -> AggregateRootT:
    """
    Exists as a syntax sugar to match `apply_events`.
    """
    aggregate.apply(event)
    return aggregate


def applied_event(event: CloudEvent, aggregate: AggregateRootT) -> CloudEvent:
    aggregate.apply(event)
    return event
