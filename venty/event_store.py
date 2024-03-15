from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable, Optional, Dict
from cloudevents.abstract import CloudEvent
from venty.types import StreamVersion, StreamName, CommitPosition
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


class EventStore:
    def append_events(
        self,
        stream_name: StreamName,
        *,
        expected_version: StreamVersion,
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


def append_event(
    repo: EventStore,
    stream_name: StreamName,
    *,
    current_version: StreamVersion,
    event: CloudEvent,
    timeout: Optional[timedelta] = None,
) -> CommitPosition:
    """
    Syntax sugar to append a single event to a stream.
    """
    return repo.append_events(
        stream_name=stream_name,
        expected_version=current_version,
        events=(event,),
        timeout=timeout,
    )


def append_events(
    repo: EventStore,
    stream_name: StreamName,
    *,
    current_version: StreamVersion,
    events: Iterable[CloudEvent],
    timeout: Optional[timedelta] = None,
) -> CommitPosition:
    """
    Syntax sugar to stay consistent with `append_event`
    """
    return repo.append_events(
        stream_name=stream_name,
        expected_version=current_version,
        events=events,
        timeout=timeout,
    )


def read_stream(
    repo: EventStore,
    stream_name: StreamName,
    *,
    stream_position: StreamVersion,
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
