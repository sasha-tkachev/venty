from typing import Optional, Any

from cloudevents.pydantic import CloudEvent

from venty.event_producer import EventProducer


class AuthEventProducer(EventProducer):
    def __init__(self, authoid: str, authtype: str, parent: EventProducer):
        self._authoid = authoid
        self._authtype = authtype
        self._parent = parent

    def produce_event(
        self, attributes: dict[str, str], data: Optional[Any]
    ) -> CloudEvent:
        if ("authid" not in attributes) and ("authtype" not in attributes):
            # https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/authcontext.md
            attributes["authid"] = self._authoid
            attributes["authtype"] = self._authtype
        return self._parent.produce_event(attributes, data)
