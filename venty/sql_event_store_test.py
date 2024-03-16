import pytest
from cloudevents.pydantic import CloudEvent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from venty.event_store import append_events, StreamState, read_stream_no_metadata
from venty.sql_event_store import Base, SqlEventStore
from venty.strong_types import NO_EVENT_VERSION
from venty.strong_types_test import dummy_events, MY_STREAM_NAME


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:", echo=False)
    # Create all tables in the engine
    Base.metadata.create_all(engine)

    return sessionmaker(engine)


def test_integration(session_factory):
    store = SqlEventStore(session_factory, CloudEvent)
    events = list(dummy_events(5))
    assert (
        append_events(
            store, MY_STREAM_NAME, expected_version=StreamState.NO_STREAM, events=events
        )
        == 4
    )
    assert (
        list(
            read_stream_no_metadata(
                store, MY_STREAM_NAME, stream_position=NO_EVENT_VERSION
            )
        )
        == events
    )


def test_read_backwards(session_factory):
    store = SqlEventStore(session_factory, CloudEvent)
    events = list(dummy_events(5))
    append_events(
        store, MY_STREAM_NAME, expected_version=StreamState.ANY, events=events
    )

    assert list(
        read_stream_no_metadata(
            store, MY_STREAM_NAME, stream_position=None, backwards=True
        )
    ) == list(reversed(events))
    assert list(
        read_stream_no_metadata(
            store, MY_STREAM_NAME, stream_position=NO_EVENT_VERSION, backwards=True
        )
    ) == list(reversed(events))

    assert store.current_version(MY_STREAM_NAME) == 4
    assert store.commit_position() == 4


def test_commit_position_of_empty_store(session_factory):
    store = SqlEventStore(session_factory, CloudEvent)
    assert store.commit_position() == -1


def test_current_position_of_non_existing_stream(session_factory):
    store = SqlEventStore(session_factory, CloudEvent)
    assert store.current_version(MY_STREAM_NAME) == StreamState.NO_STREAM
