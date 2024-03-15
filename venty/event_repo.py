from datetime import timedelta
from typing import Iterable, Optional
from cloudevents.abstract import CloudEvent

from venty.types import Version, StreamName, CommitPosition


class EventRepo:
    def append_events(
        self,
        stream_name: StreamName,
        *,
        current_version: Version,
        events: Iterable[CloudEvent],
        timeout: Optional[timedelta] = None,
    ) -> CommitPosition:
        pass


def append_event(
    repo: EventRepo,
    stream_name: StreamName,
    *,
    current_version: Version,
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
    repo: EventRepo,
    stream_name: StreamName,
    *,
    current_version: Version,
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
