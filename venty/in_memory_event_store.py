import sys
from datetime import timedelta
from typing import Iterable, Optional, Dict, List, Sequence, Union, Literal

from cloudevents.abstract import CloudEvent

from venty.event_store import EventStore, RecordedEvent, ReadInstruction, StreamState
from venty.timing import iterate_with_timeout
from venty.strong_types import (
    CommitPosition,
    StreamName,
    StreamVersion,
    NO_EVENT_VERSION,
)
from more_itertools import take


_Streams = Dict[StreamName, List[RecordedEvent]]


def _stream_version(
    stream_name: StreamName, streams: _Streams
) -> Union[StreamVersion, Literal[StreamState.NO_STREAM]]:
    events = streams.get(stream_name, None)
    if events is None:
        return StreamState.NO_STREAM
    return StreamVersion(len(events) - 1)


def _append_start_position(stream_name: StreamName, streams: _Streams) -> StreamVersion:
    version = _stream_version(stream_name, streams)
    if version == StreamState.NO_STREAM:
        return NO_EVENT_VERSION
    return version


def _expected_version_correct(
    expected_version: Union[StreamVersion, StreamState],
    stream_name: StreamName,
    streams: _Streams,
) -> bool:
    if expected_version == StreamState.ANY:
        # anything goes
        return True
    stream_version = _stream_version(stream_name, streams)
    if expected_version == StreamState.EXISTS:
        # we expect any state as long as the stream exists
        return stream_version != StreamState.NO_STREAM
    if expected_version == StreamState.NO_STREAM:
        return stream_version == StreamState.NO_STREAM
    if expected_version == NO_EVENT_VERSION:
        return stream_version in (StreamState.NO_STREAM, NO_EVENT_VERSION)
    return stream_version == expected_version


def _recorded_events(
    events: Sequence[CloudEvent],
    last_commit_position: CommitPosition,
    last_stream_position: StreamVersion,
    stream_name: StreamName,
) -> Sequence[RecordedEvent]:
    return [
        RecordedEvent(
            event=event,
            stream_name=stream_name,
            stream_position=StreamVersion(last_stream_position + 1 + i),
            commit_position=CommitPosition(last_commit_position + 1 + i),
        )
        for i, event in enumerate(events)
    ]


def _limit_position(
    events: List[RecordedEvent], position: Optional[StreamVersion]
) -> List[RecordedEvent]:
    if position is None:
        return events
    range_start = max(int(position), 0)
    return events[range_start:]


def _read_stream(
    position: Optional[StreamVersion],
    stream_name: StreamName,
    streams: _Streams,
    limit: int,
    backwards: bool,
) -> Iterable[RecordedEvent]:
    events = streams.get(stream_name)
    if not events:
        return []
    events = _limit_position(events, position)
    if backwards:
        events = reversed(events)
    return take(limit, events)


class InMemoryEventStore(EventStore):
    def __init__(self):
        self._last_commit_position = CommitPosition(-1)
        self._streams: _Streams = {}

    def attempt_append_events(
        self,
        stream_name: StreamName,
        *,
        expected_version: Union[StreamVersion, StreamState],
        events: Iterable[CloudEvent],
        timeout: Optional[timedelta] = None,
    ) -> Optional[CommitPosition]:
        if not _expected_version_correct(expected_version, stream_name, self._streams):
            return None
        consumed_events = list(iterate_with_timeout(events, timeout=timeout))
        if stream_name not in self._streams:
            self._streams[stream_name] = []
        recorded = _recorded_events(
            consumed_events,
            self._last_commit_position,
            _append_start_position(stream_name, self._streams),
            stream_name,
        )
        self._streams[stream_name].extend(recorded)
        self._last_commit_position += len(recorded)
        return self._last_commit_position

    def read_streams(
        self,
        instructions: Dict[StreamName, ReadInstruction],
        *,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[timedelta] = None,
    ) -> Iterable[RecordedEvent]:
        for stream_name, instruction in instructions.items():
            yield from _read_stream(
                position=instruction.stream_position,
                stream_name=stream_name,
                streams=self._streams,
                limit=instruction.limit,
                backwards=backwards,
            )

    def commit_position(self) -> CommitPosition:
        return self._last_commit_position

    def current_version(
        self, stream_name: StreamName, *, timeout: Optional[timedelta] = None
    ) -> Union[StreamVersion, Literal[StreamState.NO_STREAM]]:
        return _stream_version(stream_name, self._streams)
