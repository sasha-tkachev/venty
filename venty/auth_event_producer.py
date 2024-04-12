from typing import Optional, Any, Type, Dict
from venty.event_producer import EventProducer, CloudEventT, AttributeValue


class AuthEventProducer(EventProducer):
    def __init__(self, authoid: str, authtype: str, parent: EventProducer):
        self._authoid = authoid
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
            attributes["authid"] = self._authoid
            attributes["authtype"] = self._authtype
        return self._parent.produce_event(type_, data, attributes=attributes)
