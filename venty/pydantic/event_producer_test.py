from datetime import datetime

from cloudevents.sdk.event.attribute import SpecVersion
from pydantic import BaseModel

from venty.pydantic.event_producer import EventProducer
from venty.strong_types import EventSource


class MyModel(BaseModel):
    hello: str


def test_event_producer_sanity():
    producer = EventProducer(
        EventSource("my-source"),
        id_selection_algorithm=lambda: "1",
        time_selection_algorithm=lambda: datetime(year=2024, month=1, day=1),
        default_attributes={"subject": "hello"},
    )
    result = producer.produce_event({"type": "my-type"}, MyModel(hello="world"))
    assert result.dict() == {
        "data": {"hello": "world"},
        "datacontenttype": None,
        "dataschema": None,
        "id": "1",
        "source": "my-source",
        "specversion": SpecVersion.v1_0,
        "subject": "hello",
        "time": datetime(2024, 1, 1, 0, 0),
        "type": "my-type",
    }
