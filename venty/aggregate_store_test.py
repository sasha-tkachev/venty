from typing import Optional

from cloudevents.pydantic import CloudEvent

from venty.aggregate_store import AggregateStore
from venty.entity import DataclassEntity, EntityId
from venty.in_memory_event_store import InMemoryEventStore


class Book(DataclassEntity):
    name: Optional[str]

    @property
    def entity_id(self) -> EntityId:
        assert self.name is not None
        return EntityId(self.name)

    def when(self, event: CloudEvent) -> None:
        if event.get("type") == "book-created":
            self.name = event.get("subject")

    @classmethod
    def create(cls, name: str):
        event = CloudEvent.create(
            {"type": "book-created", "subject": name, "source": "dummy"}, None
        )
        result = cls()
        result.apply(event)
        return result


def test_store_load():
    hello_world = EntityId("hello-world")
    original_book = Book.create(hello_world)
    books = AggregateStore(InMemoryEventStore())
    books.store(original_book)

    loaded_book = books.load(Book, hello_world)
    assert loaded_book.name == original_book.name
