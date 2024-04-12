from typing import Literal

import pytest
from venty.cloudevent import CloudEvent
from pydantic import BaseModel

from venty.classification import may_be, must_be, must_be_list_of


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


def test_must_be_pod():
    assert must_be(str, "hello") == "hello"
    assert must_be(int, 42) == 42
    with pytest.raises(ValueError, match="Expected int, got str"):
        must_be(int, "42")
    with pytest.raises(ValueError, match="Expected int, got float"):
        must_be(int, 42.0)


def test_must_be_list_of():
    assert must_be_list_of(int, [1, 2, 3]) == [1, 2, 3]
    with pytest.raises(ValueError, match="Expected int, got str"):
        must_be_list_of(int, [1, 2, "3"])
