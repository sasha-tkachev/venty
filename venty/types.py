from typing import NewType

StreamName = NewType("StreamName", str)
Version = NewType("Version", int)
CommitPosition = NewType("CommitPosition", Version)
