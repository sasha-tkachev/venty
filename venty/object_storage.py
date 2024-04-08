from hashlib import sha256
from pathlib import Path


class ObjectStorage:
    def put(self, key: str, value: bytes):
        raise NotImplementedError()

    def get(self, key: str) -> bytes:
        raise NotImplementedError()


class FsObjectStorage(ObjectStorage):
    def __init__(self, root_dir: Path):
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def put(self, key: str, value: bytes):
        obj_file = self._root_dir / sha256(key.encode()).hexdigest()
        with obj_file.open("wb") as f:
            f.write(value)

    def get(self, key: str) -> bytes:
        obj_file = self._root_dir / sha256(key.encode()).hexdigest()
        with obj_file.open("rb") as f:
            return f.read()
