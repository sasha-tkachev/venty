from typing import List

from cloudevents.pydantic import CloudEvent
from pydantic import BaseModel, PrivateAttr

from venty.aggregate_root import AggregateRoot as _AggregateRoot
from venty.strong_types import NO_EVENT_VERSION, StreamVersion

from venty.aggregate_root import AggregateUUID  # noqa # for interfce compatibility


class AggregateRoot(BaseModel, _AggregateRoot):
    _aggregate_version: StreamVersion = PrivateAttr(NO_EVENT_VERSION)
    _uncommitted_changes: List[CloudEvent] = PrivateAttr(default_factory=list)
