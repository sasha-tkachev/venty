from venty.in_memory_event_channel import InMemoryEventChannel
from venty.strong_types_test import dummy_events


def test_all_published_events_must_be_in_the_event_channel():
    channel = InMemoryEventChannel()
    events = list(dummy_events(10))
    channel.publish(events)
    assert list(channel.published_events) == events
