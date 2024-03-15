from typing import Iterable
from uuid import UUID, uuid5
from datetime import timedelta
from venty.types import StreamName, EventType
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
) -> Iterable[CloudEvent]:
    for i in range(limit):
        time.sleep(interval.total_seconds())
        yield CloudEvent(
            {
                "type": type_,
                "source": source,
                "id": uuid5(namespace=seed, name=str(i)),
            }
        )
