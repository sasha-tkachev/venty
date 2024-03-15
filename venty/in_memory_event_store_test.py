from datetime import timedelta

import pytest

from venty.event_store import (
    read_stream_no_metadata,
    StreamState,
    RecordedEvent,
    append_events,
)
from venty.in_memory_event_store import (
    InMemoryEventStore,
    _stream_version,
    _append_start_position,
    _expected_version_correct,
)
from venty.strong_types import NO_EVENT_VERSION, StreamVersion
from venty.strong_types_test import MY_STREAM_NAME, dummy_events, YOUR_STREAM_NAME


def test_must_increase_commit_position_for_each_event_appended():
    store = InMemoryEventStore()
    amount = 5
    store.attempt_append_events(
        MY_STREAM_NAME,
        expected_version=StreamState.ANY,
        events=dummy_events(amount),
    )
    assert store.commit_position() == 4


def test_must_raise_timeout_exception_when_timeout_reached_in_iterator_consumption():
    store = InMemoryEventStore()
    with pytest.raises(TimeoutError):
        store.attempt_append_events(
            MY_STREAM_NAME,
            expected_version=StreamState.ANY,
            events=dummy_events(5, interval=timedelta(seconds=0.1)),
            timeout=timedelta(seconds=0.25),
        )


def test_must_not_raise_timeout_exception_if_deadline_was_not_reached():
    store = InMemoryEventStore()
    store.attempt_append_events(
        MY_STREAM_NAME,
        expected_version=NO_EVENT_VERSION,
        events=dummy_events(5, interval=timedelta(seconds=0.1)),
        timeout=timedelta(seconds=1),
    )


def test_stream_version_is_stream_does_not_exist_if_stream_does_not_exist():
    assert _stream_version(MY_STREAM_NAME, {}) == StreamState.NO_STREAM


def test_stream_version_is_no_stream_version_if_stream_exists_but_with_no_events():
    assert _stream_version(MY_STREAM_NAME, {MY_STREAM_NAME: []}) == NO_EVENT_VERSION


def test_append_start_position_is_minus_one_if_no_events_in_stream():
    assert _append_start_position(MY_STREAM_NAME, {}) == -1


def test_append_start_position_is_minus_one_if_stream_does_not_exist():
    assert _append_start_position(MY_STREAM_NAME, {MY_STREAM_NAME: []}) == -1


_MY_STREAM_DOES_NOT_EXIST = {}
_MY_STREAM_EXISTS_BUT_EMPTY = {MY_STREAM_NAME: []}
_MY_STREAM_EXISTS_AND_NOT_EMPTY = {
    MY_STREAM_NAME: [
        RecordedEvent(
            event=e,
            stream_name=MY_STREAM_NAME,
            stream_position=StreamVersion(i),
            commit_position=None,
        )
        for i, e in enumerate(dummy_events(3))
    ]
}


def test_expected_version_is_correct_always_if_expected_any():
    assert _expected_version_correct(
        expected_version=StreamState.ANY,
        stream_name=MY_STREAM_NAME,
        streams={},
    )
    assert _expected_version_correct(
        expected_version=StreamState.ANY,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_EXISTS_BUT_EMPTY,
    )
    assert _expected_version_correct(
        expected_version=StreamState.ANY,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_EXISTS_AND_NOT_EMPTY,
    )


def test_expected_version_is_correct_if_no_stream_is_given_and_actually_no_stream():
    assert _expected_version_correct(
        expected_version=StreamState.NO_STREAM,
        stream_name=MY_STREAM_NAME,
        streams={},
    )


def test_expected_version_is_not_correct_if_no_stream_is_given_and_actually_stream():
    assert not _expected_version_correct(
        expected_version=StreamState.NO_STREAM,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_EXISTS_BUT_EMPTY,
    )
    assert not _expected_version_correct(
        expected_version=StreamState.NO_STREAM,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_EXISTS_AND_NOT_EMPTY,
    )


def test_expected_version_is_correct_if_stream_exists_is_given_and_stream_exist_but_empty():  # noqa: E501
    assert _expected_version_correct(
        expected_version=StreamState.EXISTS,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_EXISTS_BUT_EMPTY,
    )


def test_expected_version_is_correct_if_stream_exists_is_given_and_stream_exist_and_not_empty():  # noqa: E501
    assert _expected_version_correct(
        expected_version=StreamState.EXISTS,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_EXISTS_AND_NOT_EMPTY,
    )


def test_expected_version_is_not_correct_if_stream_exists_but_stream_does_not_exist():
    assert not _expected_version_correct(
        expected_version=StreamState.EXISTS,
        stream_name=MY_STREAM_NAME,
        streams=_MY_STREAM_DOES_NOT_EXIST,
    )


def test_read_from_start():
    store = InMemoryEventStore()
    events = list(dummy_events(5))
    assert (
        append_events(
            store, MY_STREAM_NAME, expected_version=StreamState.ANY, events=events
        )
        is not None
    )
    assert (
        list(
            read_stream_no_metadata(
                store, MY_STREAM_NAME, stream_position=NO_EVENT_VERSION
            )
        )
        == events
    )


def test_read_backwards():
    store = InMemoryEventStore()
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


def test_append_events_should_create_correct_stream_versions_and_commit_offsets():
    store = InMemoryEventStore()
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
    assert {
        s: [(e.stream_position, e.commit_position) for e in events]
        for s, events in store._streams.items()
    } == {
        "my-stream": [
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),
            (5, 10),
            (6, 11),
            (7, 12),
            (8, 13),
            (9, 14),
        ],
        "your-stream": [(0, 5), (1, 6), (2, 7), (3, 8), (4, 9)],
    }
