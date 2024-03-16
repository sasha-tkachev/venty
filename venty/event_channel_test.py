from cloudevents.pydantic import CloudEvent
from mock import Mock
from venty.event_channel import best_effort_publish_event


def test_best_effort_publishing_must_not_raise_an_exception():
    channel = Mock()
    my_error = Exception("hello")
    channel.publish.side_effect = my_error
    caught = []
    best_effort_publish_event(
        CloudEvent.create({"type": "dummy", "source": "dummy"}, None),
        channel,
        on_error=lambda e: caught.append(e),
    )
    assert caught == [my_error]
