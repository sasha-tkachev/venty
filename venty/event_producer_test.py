from datetime import datetime

from cloudevents.conversion import to_dict


from venty.event_producer import SimpleEventProducer
from venty.strong_types import EventSource


def test_event_producer_sanity():
    producer = SimpleEventProducer(
        EventSource("my-source"),
        id_selection_algorithm=lambda: "1",
        time_selection_algorithm=lambda: datetime(year=2024, month=1, day=1),
        default_attributes={"subject": "hello"},
    )
    result = producer.produce_event({"type": "my-type"}, None)
    assert to_dict(result) == {
        "data": None,
        "datacontenttype": None,
        "dataschema": None,
        "id": "1",
        "source": "my-source",
        "specversion": "1.0",
        "subject": "hello",
        "time": "2024-01-01T00:00:00",
        "type": "my-type",
    }
