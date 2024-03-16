from typing import Optional, Dict, Any

from cloudevents.pydantic import CloudEvent

from venty.event_producer import (
    EventProducer as _EventProducer,
    default_time_selection_algorithm,
    default_id_selection_algorithm,
    IdSelection,
    TimeSelection,
)
from venty.strong_types import EventSource


class EventProducer(_EventProducer):
    def __init__(
        self,
        source: EventSource,
        *,
        default_attributes: Optional[Dict[str, Any]] = None,
        id_selection_algorithm: IdSelection = default_id_selection_algorithm,
        time_selection_algorithm: TimeSelection = default_time_selection_algorithm,
    ):
        super(EventProducer, self).__init__(
            source=source,
            cloudevent_cls=CloudEvent,
            default_attributes=default_attributes,
            id_selection_algorithm=id_selection_algorithm,
            time_selection_algorithm=time_selection_algorithm,
        )

    def produce_event(
        self, attributes: Dict[str, Any], data: Optional[Any]
    ) -> CloudEvent:
        return super(EventProducer, self).produce_event(attributes, data)
