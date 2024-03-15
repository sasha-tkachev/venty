from typing import Iterable
from uuid import UUID, uuid5
from datetime import timedelta, datetime, timezone
from venty.strong_types import StreamName, EventType
from cloudevents.pydantic.v2 import CloudEvent
import time

MY_STREAM_NAME = StreamName("my-stream")
YOUR_STREAM_NAME = StreamName("your-stream")
MY_EVENT_TYPE = EventType("my-type")


def dummy_events(
    limit: int,
    type_: str = MY_EVENT_TYPE,
    source: str = "dummy-source",
    seed: UUID = UUID(int=1),
    interval: timedelta = timedelta(seconds=0),
    start_time: datetime = datetime(year=2024, month=1, day=1, tzinfo=timezone.utc),
) -> Iterable[CloudEvent]:
    for i in range(limit):
        time.sleep(interval.total_seconds())
        yield CloudEvent(
            {
                "type": type_,
                "source": source,
                "id": str(uuid5(namespace=seed, name=str(i))),
                "time": (start_time + timedelta(seconds=i)).isoformat(),
            }
        )
