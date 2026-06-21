import pytest
from unittest.mock import MagicMock, patch

pytestmark = pytest.mark.unit


def _make_retry(exceptions, tries=3, delay=0.01):
    """Helper: late-import the retry decorator and apply it."""
    from common.decorators import retry as _retry
    return _retry(exceptions=exceptions, tries=tries, delay=delay)


def _make_rate_limit(calls_per_second):
    """Helper: late-import the rate_limit decorator and apply it."""
    from common.decorators import rate_limit as _rate_limit
    return _rate_limit(calls_per_second=calls_per_second)


# --- retry decorator ---


def test_retry_succeeds_on_first_attempt():
    """Function that succeeds immediately should only be called once."""
    call_count = 0

    @_make_retry(exceptions=ValueError, tries=3)
    def func():
        nonlocal call_count
        call_count += 1
        return "ok"

    result = func()
    assert result == "ok"
    assert call_count == 1


def test_retry_retries_when_exception_matches():
    """Should retry up to tries-1 times when a matching exception is raised."""
    call_count = 0

    @_make_retry(exceptions=ValueError, tries=3)
    def func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("transient")
        return "recovered"

    result = func()
    assert result == "recovered"
    assert call_count == 3


def test_retry_raises_last_exception_when_exhausted():
    """When all attempts fail, the last exception should propagate."""
    call_count = 0

    @_make_retry(exceptions=ValueError, tries=3)
    def func():
        nonlocal call_count
        call_count += 1
        raise ValueError("persistent")

    with pytest.raises(ValueError, match="persistent"):
        func()
    assert call_count == 3


def test_retry_does_not_retry_on_unmatched_exception():
    """If the raised exception type is not in 'exceptions', it propagates immediately."""
    call_count = 0

    @_make_retry(exceptions=ValueError, tries=3)
    def func():
        nonlocal call_count
        call_count += 1
        raise TypeError("wrong type")

    with pytest.raises(TypeError):
        func()
    assert call_count == 1


def test_retry_retries_on_any_of_tuple_exceptions():
    """When 'exceptions' is a tuple, any matching type triggers a retry."""
    call_count = 0

    @_make_retry(exceptions=(ValueError, KeyError), tries=3)
    def func():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("fail")
        if call_count == 2:
            raise KeyError("fail")
        return "ok"

    result = func()
    assert result == "ok"
    assert call_count == 3


@patch("common.decorators.time.sleep")
def test_retry_sleeps_between_attempts(mock_sleep):
    """The decorator should sleep for 'delay' seconds between each retry."""
    call_count = 0

    @_make_retry(exceptions=ValueError, tries=3, delay=2)
    def func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("fail")
        return "ok"

    func()
    assert mock_sleep.call_count == 2


# --- rate_limit decorator ---


@patch("common.decorators.time.sleep")
def test_rate_limit_first_call_does_not_sleep(mock_sleep):
    """The very first call should not sleep (real perf_counter is large enough)."""
    func = _make_rate_limit(calls_per_second=10)(lambda: "ok")
    result = func()
    assert result == "ok"
    mock_sleep.assert_not_called()


@patch("common.decorators.time.perf_counter")
@patch("common.decorators.time.sleep")
def test_rate_limit_rapid_second_call_sleeps(mock_sleep, mock_perf_counter):
    """Second call within the rate window should sleep for the remaining interval."""
    # Large baseline so first call sees elapsed >> min_interval and does not sleep.
    mock_perf_counter.side_effect = [1000.0, 1000.0, 1000.05, 1000.05]

    func = _make_rate_limit(calls_per_second=10)(lambda: "ok")
    func()
    func()  # elapsed ~ 0.05s < 0.1s → should sleep ~0.05s

    mock_sleep.assert_called_once()
    assert mock_sleep.call_args[0][0] == pytest.approx(0.05)


@patch("common.decorators.time.perf_counter")
@patch("common.decorators.time.sleep")
def test_rate_limit_slow_second_call_does_not_sleep(mock_sleep, mock_perf_counter):
    """Second call after the rate window has passed should not sleep."""
    mock_perf_counter.side_effect = [1000.0, 1000.0, 1001.0, 1001.0]

    func = _make_rate_limit(calls_per_second=2)(lambda: "ok")
    func()
    func()  # elapsed ~ 1.0s > 0.5s → no sleep

    mock_sleep.assert_not_called()
