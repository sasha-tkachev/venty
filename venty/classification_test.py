from typing import Literal

from cloudevents.pydantic import CloudEvent
from pydantic import BaseModel

from venty.classification import may_be


class MyData(BaseModel):
    x: int


class MyEvent(CloudEvent):
    type: Literal["my_event"] = "my_event"
    data: MyData


def test_may_be_pod():
    assert may_be(str, "hello") == "hello"
    assert may_be(int, 42) == 42
    assert may_be(int, "42") is None
    assert may_be(int, 42.0) is None


def test_may_be_cloudevent():
    event = CloudEvent.create({"type": "my_event", "source": "my-source"}, {"x": "42"})
    if result := may_be(MyEvent, event):
        assert result.data.x == 42
    else:
        raise AssertionError("Expected a result")
