import os


SQL_RECORDED_EVENTS_TABLE_NAME_KEY = "VENTY_SQL_RECORDED_EVENTS_TABLE_NAME"
SQL_RECORDED_EVENTS_TABLE_NAME_DEFAULT = "venty_recorded_events_v2"
SQL_RECORDED_EVENTS_TABLE_NAME = os.environ.get(
    SQL_RECORDED_EVENTS_TABLE_NAME_KEY, SQL_RECORDED_EVENTS_TABLE_NAME_DEFAULT
)
