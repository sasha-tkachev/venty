from datetime import timedelta, datetime

import pytest

from venty.timing import _timeout_reached, iterate_with_timeout
import time


def test_timeout_reached_if_more_time_passed_then_the_timeout_amount():
    timeout = timedelta(seconds=1)
    assert _timeout_reached(datetime.now() - (timeout * 2), timeout)


def test_timeout_not_reached_if_passed_less_time_then_the_timeout():
    timeout = timedelta(seconds=1)
    assert not _timeout_reached(datetime.now() - (timeout * 0.5), timeout)


def test_iterate_with_timeout_must_raise_timeout_error_if_iterator_consumption_takes_longer_then_timeout():  # noqa: E501
    def _foo():
        for i in range(10):
            yield i
            time.sleep(1)

    with pytest.raises(TimeoutError):
        list(iterate_with_timeout(_foo(), timeout=timedelta(seconds=2)))


def test_timeout_may_be_none():
    x = [1, 2, 3]
    assert list(iterate_with_timeout(x, timeout=None)) == x
