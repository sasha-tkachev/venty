from typing import Callable, Any

import pytest
from cloudevents.pydantic import CloudEvent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from venty.event_store import append_events, StreamState, read_stream_no_metadata
from venty.sql_event_store import Base, SqlEventStore
from venty.strong_types import NO_EVENT_VERSION, StreamVersion
from venty.strong_types_test import dummy_events, MY_STREAM_NAME, YOUR_STREAM_NAME


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
        == 5
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
    initial_commit_position = store.commit_position()
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
    assert store.commit_position() == 5
    assert store.commit_position() > initial_commit_position


def test_commit_position_of_empty_store(session_factory):
    store = SqlEventStore(session_factory, CloudEvent)
    assert store.commit_position() == 0


def test_current_position_of_non_existing_stream(session_factory):
    store = SqlEventStore(session_factory, CloudEvent)
    assert store.current_version(MY_STREAM_NAME) == StreamState.NO_STREAM


def test_append_events_should_create_correct_stream_versions_and_commit_offsets(
    session_factory,
):
    store = SqlEventStore(session_factory, CloudEvent)
    events = list(dummy_events(15))
    chunk_1 = events[:5]
    chunk_2 = events[5:10]
    chunk_3 = events[10:]
    append_events(
        store, MY_STREAM_NAME, expected_version=StreamState.NO_STREAM, events=chunk_1
    )
    append_events(
        store, YOUR_STREAM_NAME, expected_version=StreamState.NO_STREAM, events=chunk_2
    )
    append_events(
        store, MY_STREAM_NAME, expected_version=StreamVersion(4), events=chunk_3
    )
    assert (
        list(
            read_stream_no_metadata(
                store, MY_STREAM_NAME, stream_position=NO_EVENT_VERSION
            )
        )
        == chunk_1 + chunk_3
    )

    assert (
        list(
            read_stream_no_metadata(
                store, YOUR_STREAM_NAME, stream_position=NO_EVENT_VERSION
            )
        )
        == chunk_2
    )


def patch_commit(session: Session, on_commit: Callable[[], Any]):
    original = session.commit

    def _patched():
        on_commit()
        original()

    session.commit = _patched


def test_append_event_must_return_none_expected_no_stream_but_stream_exist(
    session_factory,
):  # noqa: E501
    pass


def test_append_event_must_return_none_expected_stream_exists_but_stream_does_not_exist():  # noqa: E501
    pass


def test_append_event_must_able_to_commit_to_stream_even_if_during_operation_some_other_transaction_appended_events_given_expected_any(  # noqa: E501
    session_factory,
):
    my_session = session_factory()
    your_session = session_factory()

    events = list(dummy_events(10))
    chunk_1 = events[:5]
    chunk_2 = events[5:10]

    my_store = SqlEventStore(lambda: my_session, CloudEvent)

    your_store = SqlEventStore(lambda: your_session, CloudEvent)

    patch_commit(
        my_session,
        lambda: append_events(
            your_store,
            MY_STREAM_NAME,
            expected_version=StreamState.ANY,
            events=chunk_1,
        ),
    )

    append_events(
        my_store,
        MY_STREAM_NAME,
        expected_version=StreamState.ANY,
        events=chunk_2,
    )

    assert (
        list(
            read_stream_no_metadata(
                my_store, MY_STREAM_NAME, stream_position=NO_EVENT_VERSION
            )
        )
        == events
    )


def test_append_event_must_able_to_commit_to_stream_even_if_during_operation_some_other_transaction_appended_events_given_expected_stream_exists():  # noqa: E501
    pass


def test_append_event_must_return_none_even_if_during_operation_some_other_transaction_appended_events_given_expected_stream_not_exists():  # noqa: E501
    pass
