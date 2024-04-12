# Venty Settings

Venty is configurable through env variables

### `VENTY_SQL_STREAMS_TABLE_NAME`
Used by the [SqlEventStore](sql_event_store.py) to decide what is the table name 
which will contains all the stream definitions of the event store.

Default: `venty_streams`

### `VENTY_SQL_RECORDED_EVENTS_TABLE_NAME`
Used by the [SqlEventStore](sql_event_store.py) to decide what is the table name 
which will contains all recorded events in the event store.

Default: `venty_recorded_events_v2`