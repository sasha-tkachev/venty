from typing import Optional
from uuid import UUID, uuid5

from cloudevents.pydantic import CloudEvent

from venty.aggregate_store import AggregateStore
from venty.aggregate_root import AggregateUUID, AggregateRoot
from venty.in_memory_event_store import InMemoryEventStore

_BOOKS_NAMESPACE = UUID("c3ec5a4e-5e4f-44bf-ac40-bfb6c52cbdf6")


def _book_uuid(name: str) -> AggregateUUID:
    return AggregateUUID(uuid5(_BOOKS_NAMESPACE, name))


class Book(AggregateRoot):
    name: Optional[str]
    checked_out_by: Optional[str]

    @property
    def aggregate_uuid(self) -> AggregateUUID:
        assert self.name is not None
        return _book_uuid(self.name)

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
    the_idiot = "The Idiot"
    original_book = Book.create(the_idiot)
    library = AggregateStore(InMemoryEventStore())
    library.store(original_book)

    loaded_book = library.load(Book, _book_uuid(the_idiot))
    assert loaded_book.name == original_book.name

    loaded_book.check_out("Alice")
    library.store(loaded_book)

    original_book = library.fetch(original_book)

    assert original_book.checked_out_by == "Alice"
