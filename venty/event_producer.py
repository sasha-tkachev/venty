from datetime import datetime
from typing import Any, Dict, Optional, Callable

from cloudevents.pydantic import CloudEvent
from cloudevents.sdk.event.attribute import (
    default_id_selection_algorithm,
    default_time_selection_algorithm,
)

from venty.strong_types import EventSource


def _ignore_invalid_attributes(attributes: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: v for k, v in attributes.items() if k not in {"data", "id", "source", "time"}
    }


IdSelection = Callable[[], str]
TimeSelection = Callable[[], datetime]


class EventProducer:
    def __init__(
        self,
        source: EventSource,
        *,
        default_attributes: Optional[Dict[str, Any]] = None,
        id_selection_algorithm: IdSelection = default_id_selection_algorithm,
        time_selection_algorithm: TimeSelection = default_time_selection_algorithm,
    ):
        self._source = source
        if default_attributes is None:
            default_attributes = {}
        self._default_attributes = _ignore_invalid_attributes(default_attributes)
        self._id_selection_algorithm = id_selection_algorithm
        self._time_selection_algorithm = time_selection_algorithm

    def produce_event(
        self, attributes: Dict[str, Any], data: Optional[Any]
    ) -> CloudEvent:
        actual_attributes = self._default_attributes.copy()
        actual_attributes.update(attributes)
        actual_attributes["source"] = self._source
        actual_attributes["id"] = self._id_selection_algorithm()
        actual_attributes["time"] = self._time_selection_algorithm()
        return CloudEvent.create(actual_attributes, data)
