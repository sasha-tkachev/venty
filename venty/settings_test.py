from pathlib import Path
import pytest
from venty import settings


_docs = (Path(__file__).parent / "settings.md").read_text()

_all_defaults = {v for k, v in settings.__dict__.items() if k.endswith("_DEFAULT")}
_all_keys = {v for k, v in settings.__dict__.items() if k.endswith("_KEY")}


@pytest.mark.parametrize("default_value", _all_defaults)
def test_defaults_are_documented(default_value):
    assert "`{}`".format(default_value) in _docs


@pytest.mark.parametrize("key", _all_keys)
def test_key_names_are_documented(key):
    assert "`{}`".format(key) in _docs
