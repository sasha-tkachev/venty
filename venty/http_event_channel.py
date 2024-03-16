from enum import Enum
from typing import Iterable, Callable, Tuple, Dict

from cloudevents.abstract import CloudEvent
from cloudevents.conversion import to_structured, to_binary

try:
    from requests import Session
except ImportError:
    raise RuntimeError(
        "Venty http feature is not installed. Install it "
        "using pip install venty[http]"
    )
from venty.event_channel import EventChannel


class HttpChannelMode(Enum):
    BINARY = "BINARY"
    STRUCTURED = "STRUCTURED"


def _choose_strategy(
    mode: HttpChannelMode,
) -> Callable[[CloudEvent], Tuple[Dict[str, str], bytes]]:
    if mode == HttpChannelMode.STRUCTURED:
        return to_structured
    if mode == HttpChannelMode.BINARY:
        return to_binary
    raise NotImplementedError()


class HttpEventChannel(EventChannel):
    def __init__(
        self,
        base_url: str,
        session: Session,
        mode: HttpChannelMode = HttpChannelMode.BINARY,
    ):
        self._base_url = base_url
        self._session = session
        self._strategy = _choose_strategy(mode)

    def publish(self, events: Iterable[CloudEvent]) -> None:
        for event in events:
            headers, body = self._strategy(event)
            self._session.post(self._base_url, body, headers=headers)
