from typing import Iterable

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
