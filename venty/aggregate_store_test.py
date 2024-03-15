from typing import Optional

from cloudevents.pydantic import CloudEvent

from venty.aggregate_store import AggregateStore
from venty.entity import DataclassEntity, EntityId
from venty.in_memory_event_store import InMemoryEventStore


class Book(DataclassEntity):
    name: Optional[str]
    checked_out_by: Optional[str]

    @property
    def entity_id(self) -> EntityId:
        assert self.name is not None
        return EntityId(self.name)

    def when(self, event: CloudEvent) -> None:
        if event.get("type") == "book-created":
            self.name = event.get("subject")
        if event.get("type") == "book-checked-out":
            self.checked_out_by = event.get("source")
        if event.get("type") == "book-returned":
            self.checked_out_by = None

    @classmethod
    def create(cls, name: str):
        event = CloudEvent.create(
            {"type": "book-created", "subject": name, "source": "library"}, None
        )
        result = cls()
        result.apply(event)
        return result

    def check_out(self, patron: str):
        self.apply(
            CloudEvent.create({"type": "book-checked-out", "source": patron}, None)
        )

    def return_book(self):
        self.apply(
            CloudEvent.create({"type": "book-returned", "source": "any-patron"}, None)
        )


def test_store_integration():
    the_idiot = EntityId("The Idiot")
    original_book = Book.create(the_idiot)
    library = AggregateStore(InMemoryEventStore())
    library.store(original_book)

    loaded_book = library.load(Book, the_idiot)
    assert loaded_book.name == original_book.name

    loaded_book.check_out("Alice")
    library.store(loaded_book)

    original_book = library.fetch(original_book)

    assert original_book.checked_out_by == "Alice"
