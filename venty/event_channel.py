from typing import Iterable, Callable

from cloudevents.abstract import CloudEvent


class EventChannel:
    """
    A channel is a mechanism created by the server for the organization and transmission
    of messages. Users can define channels as a topic, queue, routing key, path,
    or subject depending on the protocol used.
    Based on the asyncapi concept https://www.asyncapi.com/docs/concepts/channel
    """

    def publish(self, events: Iterable[CloudEvent]) -> None:
        raise NotImplementedError()


def publish_events(events: Iterable[CloudEvent], channel: EventChannel) -> None:
    channel.publish(events)


def publish_event(event: CloudEvent, channel: EventChannel) -> None:
    channel.publish((event,))


def best_effort_publish_events(
    events: Iterable[CloudEvent],
    channel: EventChannel,
    *,
    on_error: Callable[[Exception], None] = lambda e: None
) -> None:
    try:
        publish_events(events, channel)
    except Exception as e:
        on_error(e)


def best_effort_publish_event(
    event: CloudEvent,
    channel: EventChannel,
    *,
    on_error: Callable[[Exception], None] = lambda e: None
):
    try:
        publish_event(event, channel)
    except Exception as e:
        on_error(e)
