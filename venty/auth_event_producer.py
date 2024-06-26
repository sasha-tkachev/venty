from typing import Optional, Any, Type, Dict
from venty.event_producer import EventProducer, AttributeValue
from venty.strong_types import CloudEventT


class AuthEventProducer(EventProducer):
    def __init__(self, authid: str, authtype: str, parent: EventProducer):
        self._authid = authid
        self._authtype = authtype
        self._parent = parent

    def produce_event(
        self,
        type_: Type[CloudEventT],
        data: Optional[Any],
        *,
        attributes: Optional[Dict[str, AttributeValue]] = None
    ) -> CloudEventT:
        if attributes is None:
            attributes = {}
        if ("authid" not in attributes) and ("authtype" not in attributes):
            # https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/authcontext.md
            attributes["authid"] = self._authid
            attributes["authtype"] = self._authtype
        return self._parent.produce_event(type_, data, attributes=attributes)
