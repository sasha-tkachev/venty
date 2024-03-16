from cloudevents.pydantic import CloudEvent
from mock import Mock

from venty.event_channel import publish_event
from venty.http_event_channel import HttpEventChannel, HttpChannelMode


def test_http_channel_with_binary_mode_must_put_all_ce_attributes_in_header():
    session = Mock()
    channel = HttpEventChannel(
        "https://localhost:1337", session, HttpChannelMode.BINARY
    )
    publish_event(
        CloudEvent(
            {
                "type": "my-type",
                "source": "my-source",
                "id": "my-id",
                "time": "2024-01-01",
            },
            {"hello": "world"},
        ),
        channel,
    )
    session.post.assert_called_once_with(
        "https://localhost:1337",
        b'{"hello": "world"}',
        headers={
            "ce-specversion": "1.0",
            "ce-id": "my-id",
            "ce-source": "my-source",
            "ce-type": "my-type",
            "ce-time": "2024-01-01T00:00:00",
        },
    )


def test_http_channel_with_structured_mode_must_put_event_as_json_in_body():
    session = Mock()
    channel = HttpEventChannel(
        "https://localhost:1337", session, HttpChannelMode.STRUCTURED
    )
    publish_event(
        CloudEvent(
            {
                "type": "my-type",
                "source": "my-source",
                "id": "my-id",
                "time": "2024-01-01",
            },
            {"hello": "world"},
        ),
        channel,
    )
    session.post.assert_called_once_with(
        "https://localhost:1337",
        b'{"specversion": "1.0", "id": "my-id", "source": "my-source", "type": '
        b'"my-type", "time": "2024-01-01T00:00:00", "data": {"hello": "world"}}',
        headers={"content-type": "application/cloudevents+json"},
    )
