from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Iterable, Optional, Dict, Union, Literal, Callable
from cloudevents.abstract import CloudEvent
from venty.strong_types import (
    StreamVersion,
    StreamName,
    CommitPosition,
    NO_EVENT_VERSION,
)
import sys


@dataclass(frozen=True)
class ReadInstruction:
    stream_position: Optional[StreamVersion]
    limit: int = sys.maxsize


@dataclass(frozen=True)
class RecordedEvent:
    event: CloudEvent
    stream_name: StreamName
    stream_position: StreamVersion
    commit_position: Optional[CommitPosition]


class StreamState(Enum):
    ANY = "ANY"
    NO_STREAM = "NO_STREAM"
    EXISTS = "EXISTS"


ExpectedVersion = Union[StreamVersion, StreamState]


def is_stream_version_correct(
    expected_version: Union[StreamVersion, StreamState],
    stream_version: Callable[[], Union[StreamVersion, Literal[StreamState.NO_STREAM]]],
) -> bool:
    if expected_version == StreamState.ANY:
        #  we don't even need to query the stream version
        return True
    stream_version = stream_version()
    if expected_version == StreamState.EXISTS:
        # we expect any state as long as the stream exists
        return stream_version != StreamState.NO_STREAM
    if expected_version == StreamState.NO_STREAM:
        return stream_version == StreamState.NO_STREAM
    if expected_version == NO_EVENT_VERSION:
        return stream_version in (StreamState.NO_STREAM, NO_EVENT_VERSION)
    return stream_version == expected_version


class EventStore:
    def attempt_append_events(
        self,
        stream_name: StreamName,
        *,
        expected_version: ExpectedVersion,
        events: Iterable[CloudEvent],
        timeout: Optional[timedelta] = None,
    ) -> Optional[CommitPosition]:
        """
        :param stream_name: unique identifier of a stream inside this event store.
            The stream name is unique only in the context of this event store.
            You MUST NOT assume it is globally unique.
        :param expected_version: The version of the stream before the events were
            committed to it, if the actual version of the stream does not match this
            value the events will not be appended to it and no commit position will
            be returned from this function.
        :param events: events which should be committed to the stream at the given
            version.
        :param timeout: once this duration passed from the start of the execution
            this function MAY raise TimeoutError.
            If the TimeoutError exception was raised, events MUST NOT be committed to
            the stream.
        :return: If the append operation succeeded will return the event store global
            commit position of the events.
            If the expected version was wrong, None MUST be returned.
        """
        raise NotImplementedError()

    def read_streams(
        self,
        instructions: Dict[StreamName, ReadInstruction],
        *,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[timedelta] = None,
    ) -> Iterable[RecordedEvent]:
        raise NotImplementedError()

    def commit_position(self) -> CommitPosition:
        raise NotImplementedError()

    def current_version(
        self, stream_name: StreamName, *, timeout: Optional[timedelta] = None
    ) -> Optional[Union[StreamVersion, Literal[StreamState.NO_STREAM]]]:
        raise NotImplementedError()


def attempt_append_event(
    repo: EventStore,
    stream_name: StreamName,
    *,
    expected_version: ExpectedVersion,
    event: CloudEvent,
    timeout: Optional[timedelta] = None,
) -> Optional[CommitPosition]:
    """
    Syntax sugar to append a single event to a stream.
    """
    return repo.attempt_append_events(
        stream_name=stream_name,
        expected_version=expected_version,
        events=(event,),
        timeout=timeout,
    )


def attempt_append_events(
    repo: EventStore,
    stream_name: StreamName,
    *,
    expected_version: ExpectedVersion,
    events: Iterable[CloudEvent],
    timeout: Optional[timedelta] = None,
) -> Optional[CommitPosition]:
    """
    Syntax sugar to stay consistent with `append_event`
    """
    return repo.attempt_append_events(
        stream_name=stream_name,
        expected_version=expected_version,
        events=events,
        timeout=timeout,
    )


class WrongExpectedVersion(ValueError):
    pass


def append_events(
    repo: EventStore,
    stream_name: StreamName,
    *,
    expected_version: ExpectedVersion,
    events: Iterable[CloudEvent],
    timeout: Optional[timedelta] = None,
) -> CommitPosition:
    """
    Syntax sugar to stay consistent with `append_event`
    """
    result = repo.attempt_append_events(
        stream_name=stream_name,
        expected_version=expected_version,
        events=events,
        timeout=timeout,
    )
    if result is None:
        raise WrongExpectedVersion()
    return result


def append_event(
    repo: EventStore,
    stream_name: StreamName,
    *,
    expected_version: ExpectedVersion,
    event: CloudEvent,
    timeout: Optional[timedelta] = None,
) -> Optional[CommitPosition]:
    """
    Syntax sugar to append a single event to a stream.
    """
    return append_events(
        repo=repo,
        stream_name=stream_name,
        expected_version=expected_version,
        events=(event,),
        timeout=timeout,
    )


def read_stream(
    repo: EventStore,
    stream_name: StreamName,
    *,
    stream_position: Optional[StreamVersion],
    limit: int = sys.maxsize,
    backwards: bool = False,
    timeout: Optional[timedelta] = None,
) -> Iterable[RecordedEvent]:
    return repo.read_streams(
        {
            stream_name: ReadInstruction(
                stream_position=stream_position,
                limit=limit,
            )
        },
        backwards=backwards,
        timeout=timeout,
    )


def read_stream_no_metadata(
    repo: EventStore,
    stream_name: StreamName,
    *,
    stream_position: Optional[StreamVersion],
    limit: int = sys.maxsize,
    backwards: bool = False,
    timeout: Optional[timedelta] = None,
) -> Iterable[CloudEvent]:
    return (
        e.event
        for e in read_stream(
            repo,
            stream_name,
            stream_position=stream_position,
            limit=limit,
            backwards=backwards,
            timeout=timeout,
        )
    )
