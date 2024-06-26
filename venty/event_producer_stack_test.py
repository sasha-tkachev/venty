from typing import Literal

import pytest
from venty.cloudevent import CloudEvent

from venty.event_producer import SimpleEventProducer
from venty.event_producer_stack import EventProducerStack
from venty.strong_types import EventSource


class MyType(CloudEvent):
    type: Literal["my-type"] = "my-type"


class YourType(CloudEvent):
    type: Literal["your-type"] = "your-type"


def test_event_producer_stack():
    my_producer = SimpleEventProducer(source=EventSource("my-source"))
    your_producer = SimpleEventProducer(source=EventSource("your-source"))
    stack = EventProducerStack(my_producer)

    a = stack.produce_event(MyType, None)
    assert a.source == "my-source"
    with stack.scoped_event_producer(your_producer):
        b = stack.produce_event(YourType, None)
        assert b.source == "your-source"
    c = stack.produce_event(MyType, None)
    assert c.source == "my-source"


def test_event_producer_stack_should_pop_on_exception():
    my_producer = SimpleEventProducer(source=EventSource("my-source"))
    your_producer = SimpleEventProducer(source=EventSource("your-source"))
    stack = EventProducerStack(my_producer)

    a = stack.produce_event(MyType, None)
    assert a.source == "my-source"
    with pytest.raises(ValueError):
        with stack.scoped_event_producer(your_producer):
            b = stack.produce_event(YourType, None)
            assert b.source == "your-source"
            raise ValueError("boom")
    c = stack.produce_event(MyType, None)
    assert c.source == "my-source"
