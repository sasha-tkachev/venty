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
        current_version: StreamVersion,
        events: Iterable[CloudEvent],
        timeout: Optional[timedelta] = None,
    ) -> CommitPosition:
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
        current_version=current_version,
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
        current_version=current_version,
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
