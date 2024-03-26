import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from logging import Formatter, LogRecord, StreamHandler
from typing import Any, Dict, TypeVar, Optional

from cloudevents.pydantic import CloudEvent
from pythonjsonlogger.jsonlogger import JsonFormatter
import json
from venty.event_producer import EventProducer

# https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/severity.md
_FATAL_SEVERITY = 21
_ERROR_SEVERITY = 17
_WARNING_SEVERITY = 13
_INFO_SEVERITY = 9
_DEBUG_SEVERITY = 5
_TRACE_SEVERITY = 1


_FATAL_SEVERITY_TEXT = "FATAL"
_ERROR_SEVERITY_TEXT = "ERROR"
_WARNING_SEVERITY_TEXT = "WARNING"
_INFO_SEVERITY_TEXT = "INFO"
_DEBUG_SEVERITY_TEXT = "DEBUG"
_TRACE_SEVERITY_TEXT = "TRACE"


def _severity_text(severity_num: int) -> str:
    if severity_num < _DEBUG_SEVERITY:
        return _TRACE_SEVERITY_TEXT
    if severity_num < _INFO_SEVERITY:
        return _DEBUG_SEVERITY_TEXT
    if severity_num < _WARNING_SEVERITY:
        return _INFO_SEVERITY_TEXT
    if severity_num < _ERROR_SEVERITY:
        return _WARNING_SEVERITY_TEXT
    if severity_num < _FATAL_SEVERITY:
        return _ERROR_SEVERITY_TEXT
    if severity_num >= _FATAL_SEVERITY:
        return _FATAL_SEVERITY_TEXT
    return _TRACE_SEVERITY_TEXT


def __severity_number(level: int) -> int:
    if logging.NOTSET <= level < logging.DEBUG:
        return _TRACE_SEVERITY
    if logging.DEBUG <= level < logging.INFO:
        return _DEBUG_SEVERITY
    if logging.INFO <= level < logging.WARNING:
        return _INFO_SEVERITY
    if logging.WARNING <= level < logging.ERROR:
        return _WARNING_SEVERITY
    if logging.ERROR <= level < logging.FATAL:
        return _ERROR_SEVERITY
    if logging.FATAL <= level:
        return _FATAL_SEVERITY


@contextmanager
def _no_logging():
    old = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        yield
    finally:
        logging.disable(old)


_CLOUDEVENT_MUST_HAVE_ATTRIBUTES = {"specversion", "type", "source", "id"}


def _is_dict_cloudevent(data: Dict[str, Any]) -> bool:
    return all(k in data for k in _CLOUDEVENT_MUST_HAVE_ATTRIBUTES)


T = TypeVar("T")
K = TypeVar("K")


def _pop_if_exists(a_dict: Dict[T, K], key: T) -> Optional[K]:
    if key in a_dict:
        return a_dict.pop(key)
    return None


def _log_message(data: Dict[str, Any]) -> Optional[str]:
    result = data.get("message")
    if result is not None:
        if isinstance(result, str):
            return result
    return None


def _is_string_cloudevent(message: Optional[str]) -> bool:
    if message is None:
        return False
    message = message.strip()
    if not message.startswith("{") or not message.endswith("}"):
        return False
    if any(k not in message for k in _CLOUDEVENT_MUST_HAVE_ATTRIBUTES):
        return False
    try:
        return _is_dict_cloudevent(json.loads(message))
    except json.JSONDecodeError:
        return False


_DEFAULT_LOG_TYPE = "venty.LogRecorded"


def _record_event_type(data: Dict[str, Any]) -> str:
    return data.get("type", _DEFAULT_LOG_TYPE)


def _record_time(record: LogRecord) -> datetime:
    return datetime.fromtimestamp(record.created, tz=timezone.utc)


def _event_attributes(record: LogRecord, data: Dict[str, Any]) -> Dict[str, Any]:
    severity_number = __severity_number(record.levelno)
    return {
        "type": _record_event_type(data),
        "time": _record_time(record).isoformat(),
        "severitytext": _severity_text(severity_number),
        "severitynumber": severity_number,
    }


class VentyFormatter(Formatter):
    def __init__(self, *args, producer: EventProducer, **kwargs):
        super().__init__(*args, **kwargs)
        self._data_formatter = JsonFormatter(*args, **kwargs)
        self._producer = producer

    def format(self, record: LogRecord):
        with _no_logging():
            data = json.loads(self._data_formatter.format(record))
            if _is_dict_cloudevent(data):
                _pop_if_exists(data, "message")
                result = CloudEvent.parse_obj(data)
            elif _is_string_cloudevent(
                _log_message(data)
            ):  # an event was given in the message
                message = _log_message(data)
                assert message is not None
                result = CloudEvent.parse_raw(message)
            else:
                result = self._producer.produce_event(
                    _event_attributes(record, data),
                    data,
                )
            return result.json(exclude_none=True)


def venty_log_handler(
    producer: EventProducer,
    level: int = logging.INFO,
) -> StreamHandler:
    result = StreamHandler()
    result.setFormatter(VentyFormatter(producer))
    result.setLevel(level)
    return result
