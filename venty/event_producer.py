from datetime import datetime
from typing import Any, Dict, Optional, Type, Callable

from cloudevents.abstract import CloudEvent
from cloudevents.sdk.event.attribute import (
    default_id_selection_algorithm,
    default_time_selection_algorithm,
)

from venty.strong_types import EventSource


def _ignore_invalid_attributes(attributes: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: v for k, v in attributes.items() if k not in {"data", "id", "source", "time"}
    }


class EventProducer:
    def __init__(
        self,
        source: EventSource,
        *,
        cloudevent_cls: Type[CloudEvent],
        default_attributes: Optional[Dict[str, Any]] = None,
        id_selection_algorithm: Callable[[], str] = default_id_selection_algorithm,
        time_selection_algorithm: Callable[
            [], datetime
        ] = default_time_selection_algorithm,
    ):
        self._source = source
        self._cloudevent_cls = cloudevent_cls
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
        return self._cloudevent_cls.create(actual_attributes, data)
