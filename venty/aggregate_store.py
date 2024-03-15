from typing import Type

from venty import EventStore
from venty.event_store import append_events, read_stream_no_metadata
from venty.entity import Entity, EntityId, EntityT
from venty.strong_types import StreamName


def _entity_stream(entity_cls: Type[Entity], entity_id: EntityId) -> StreamName:
    return StreamName("{}-{}".format(entity_cls.entity_type(), entity_id))


class AggregateStore:
    def __init__(self, event_store: EventStore):
        self._event_store = event_store

    def store(self, entity: Entity):
        append_events(
            self._event_store,
            _entity_stream(type(entity), entity.entity_id),
            expected_version=entity.entity_version,
            events=entity.entity_changes,
        )
        entity.clear_entity_changes()

    def load(self, entity_cls: Type[EntityT], entity_id: EntityId) -> EntityT:
        result: Entity = entity_cls()
        result.load(
            read_stream_no_metadata(
                self._event_store,
                _entity_stream(entity_cls, entity_id),
                stream_position=None,
            )
        )
        return result

    def fetch(self, entity: EntityT) -> EntityT:
        entity.load(
            read_stream_no_metadata(
                self._event_store,
                _entity_stream(type(entity), entity.entity_id),
                stream_position=entity.entity_version,
            )
        )
        return entity
