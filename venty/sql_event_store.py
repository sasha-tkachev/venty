from time import time
from sqlalchemy.exc import IntegrityError

from venty.settings import SQL_STREAMS_TABLE_NAME, SQL_RECORDED_EVENTS_TABLE_NAME
from venty.timing import assert_timeout_not_supported

try:
    import sqlalchemy
except ImportError:  # pragma: no cover # hard to test
    raise RuntimeError(
        "Venty sql feature is not installed. " "Install it using pip install venty[sql]"
    )


from datetime import timedelta
from sqlalchemy import and_, or_, func
from typing import (
    Iterable,
    Optional,
    Union,
    Literal,
    Callable,
    Sequence,
    Tuple,
    Dict,
    Type,
)

from cloudevents.abstract import CloudEvent, AnyCloudEvent
from cloudevents.conversion import to_json, from_json
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session, aliased

from venty import EventStore
from venty.event_store import (
    ExpectedVersion,
    is_stream_version_correct,
    StreamState,
    ReadInstruction,
    RecordedEvent,
)
from venty.strong_types import (
    StreamName,
    CommitPosition,
    StreamVersion,
    NO_EVENT_VERSION,
)

Base = declarative_base()


class StreamRow(Base):
    __tablename__ = SQL_STREAMS_TABLE_NAME
    id: int = Column(Integer, primary_key=True)
    stream_name: StreamName = Column(String(255), nullable=False, unique=True)


class RecordedEventRow(Base):
    __tablename__ = SQL_RECORDED_EVENTS_TABLE_NAME
    id: CommitPosition = Column(Integer, primary_key=True)
    stream_id: int = Column(Integer, ForeignKey("{}.id".format(SQL_STREAMS_TABLE_NAME)))
    stream_position: StreamVersion = Column(Integer, nullable=False)  # TODO: rename
    event: str = Column(Text, nullable=False)  # Added payload field here

    __table_args__ = (
        UniqueConstraint(
            "stream_id", "stream_position", name="_stream_id_stream_position_uc"
        ),
    )


def _stream_metadata(
    stream_name: StreamName, session: Session
) -> Tuple[Union[StreamVersion, Literal[StreamState.NO_STREAM]], Optional[int]]:
    recorded_event_alias = aliased(RecordedEventRow)

    stream_with_highest_position = (
        session.query(
            func.max(recorded_event_alias.stream_position).label(
                "highest_stream_position"
            ),
            StreamRow.id,
        )
        .join(
            recorded_event_alias,
            StreamRow.id == recorded_event_alias.stream_id,
            isouter=True,
        )
        .filter(StreamRow.stream_name == stream_name)
        .group_by(StreamRow.id)
        .first()
    )

    # Check if the stream exists and return the appropriate values
    if stream_with_highest_position is None:
        return StreamState.NO_STREAM, None
    else:
        highest_stream_position, stream_id = stream_with_highest_position
        return StreamVersion(highest_stream_position), stream_id


def _record_event_rows(
    events: Sequence[CloudEvent],
    last_stream_position: Union[StreamVersion, Literal[StreamState.NO_STREAM]],
    stream_id: int,
) -> Sequence[RecordedEventRow]:
    return [
        RecordedEventRow(
            stream_id=stream_id,
            stream_position=last_stream_position + 1 + i,
            event=to_json(event),
        )
        for i, event in enumerate(events)
    ]


def _row_to_recorded_event(
    event_row: RecordedEventRow, stream_row: StreamRow, event_type: Type[AnyCloudEvent]
) -> RecordedEvent:
    return RecordedEvent(
        commit_position=CommitPosition(
            event_row.id,
        ),
        stream_position=StreamVersion(
            event_row.stream_position,
        ),
        stream_name=StreamName(
            stream_row.stream_name,
        ),
        event=from_json(
            event_type,
            event_row.event,
        ),
    )


def _query_streams(
    session: Session,
    instructions: Dict[StreamName, ReadInstruction],
    backwards: bool,
    event_type: Type[AnyCloudEvent],
) -> Iterable[RecordedEvent]:
    or_conditions = [
        and_(
            StreamRow.stream_name == stream_name,
            RecordedEventRow.stream_position >= instruction.stream_position_or_default,
            RecordedEventRow.stream_position
            <= instruction.stream_position_or_default + instruction.limit,
        )
        for stream_name, instruction in instructions.items()
    ]

    # Construct the query
    return (
        _row_to_recorded_event(recorded_row, stream_row, event_type)
        for recorded_row, stream_row in session.query(RecordedEventRow, StreamRow)
        .join(StreamRow, StreamRow.id == RecordedEventRow.stream_id)
        .filter(or_(*or_conditions))
        .order_by(
            RecordedEventRow.stream_position.desc()
            if backwards
            else RecordedEventRow.stream_position.asc()
        )
    )


def _highest_commit_position(row_records: Iterable[RecordedEventRow]) -> CommitPosition:
    return CommitPosition(max([int(r.id) for r in row_records]))


def _last_stream_position(
    stream_position: Union[StreamVersion, Literal[StreamState.NO_STREAM]]
) -> StreamVersion:
    if stream_position == StreamState.NO_STREAM:
        return NO_EVENT_VERSION
    return stream_position  # type: ignore


def _create_stream(stream_name: StreamName, session: Session) -> int:
    stream = StreamRow(
        id=int(time() * 1000),  # it is ok if we have a race, we will just re-try
        stream_name=stream_name,
    )
    session.add(stream)
    return stream.id


def _commit_append_events(
    stream_name: StreamName,
    expected_version: ExpectedVersion,
    events: Sequence[CloudEvent],
    session: Session,
) -> Optional[CommitPosition]:
    stream_version, stream_id = _stream_metadata(stream_name, session)
    if not is_stream_version_correct(expected_version, lambda: stream_version):
        return None
    if stream_id is None:
        stream_id = _create_stream(stream_name, session)
        session.commit()

    row_records = _record_event_rows(
        events=events,
        last_stream_position=_last_stream_position(stream_version),
        stream_id=stream_id,
    )
    session.add_all(row_records)
    session.commit()
    return _highest_commit_position(row_records)


class SqlEventStore(EventStore):

    def __init__(
        self, session_factory: Callable[[], Session], event_type: Type[AnyCloudEvent]
    ):
        self._session_factory = session_factory
        self._event_type = event_type

    def attempt_append_events(
        self,
        stream_name: StreamName,
        *,
        expected_version: ExpectedVersion,
        events: Iterable[CloudEvent],
        timeout: Optional[timedelta] = None,
    ) -> Optional[CommitPosition]:
        assert_timeout_not_supported(timeout)
        consumed_events = list(events)
        while True:
            with self._session_factory() as session:
                try:
                    return _commit_append_events(
                        stream_name, expected_version, consumed_events, session
                    )
                except IntegrityError as e:
                    session.rollback()

    def read_streams(
        self,
        instructions: Dict[StreamName, ReadInstruction],
        *,
        backwards: bool = False,
        timeout: Optional[timedelta] = None,
    ) -> Iterable[RecordedEvent]:
        assert_timeout_not_supported(timeout)
        with self._session_factory() as session:
            return _query_streams(session, instructions, backwards, self._event_type)

    def commit_position(self) -> CommitPosition:
        with self._session_factory() as session:
            result = session.query(func.max(RecordedEventRow.id)).scalar()
            if result is None:
                return CommitPosition(0)
            return CommitPosition(result)

    def current_version(
        self, stream_name: StreamName, *, timeout: Optional[timedelta] = None
    ) -> Optional[Union[StreamVersion, Literal[StreamState.NO_STREAM]]]:
        assert_timeout_not_supported(timeout)
        with self._session_factory() as session:
            highest_stream_version, _ = _stream_metadata(stream_name, session)
            return highest_stream_version
