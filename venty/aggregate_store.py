from typing import Type

from venty import EventStore
from venty.event_store import append_events, read_stream_no_metadata
from venty.aggregate_root import AggregateRoot, AggregateUUID, AggregateRootT
from venty.strong_types import StreamName, StreamVersion


def _aggregate_stream(uuid: AggregateUUID) -> StreamName:
    return StreamName(str(uuid))


class AggregateStore:
    def __init__(self, event_store: EventStore):
        self._event_store = event_store

    def store(self, aggregate: AggregateRoot):
        append_events(
            self._event_store,
            _aggregate_stream(aggregate.aggregate_uuid()),
            expected_version=aggregate.aggregate_version(),
            events=aggregate.uncommitted_changes(),
        )
        aggregate.mark_changes_as_committed()

    def load(
        self, aggregate_cls: Type[AggregateRootT], uuid: AggregateUUID
    ) -> AggregateRootT:
        result: AggregateRoot = aggregate_cls()
        result.load_from_history(
            read_stream_no_metadata(
                self._event_store,
                _aggregate_stream(uuid),
                stream_position=None,
            )
        )
        return result
