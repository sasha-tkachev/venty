try:
    import pydantic
except ImportError:  # pragma: no cover # hard to test
    raise RuntimeError(
        "Venty pydantic feature is not installed. "
        "Install it using pip install venty[pydantic]"
    )
