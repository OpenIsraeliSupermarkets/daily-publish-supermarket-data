"""utils.retry.retry_on_429 (#167). No network, no real sleeping."""

import unittest
from types import SimpleNamespace
from unittest import mock

from utils.retry import is_rate_limited, retry_on_429


class FakeHTTPError(Exception):
    """Mimics requests/kagglehub HTTP errors: carries a .response."""

    def __init__(self, status, headers=None):
        reason = "Too Many Requests" if status == 429 else "Error"
        super().__init__(f"{status} Client Error: {reason} for url: https://x")
        self.response = SimpleNamespace(status_code=status, headers=headers or {})


def rate_limited(headers=None):
    return FakeHTTPError(429, headers)


class TestRetryOnRateLimit(unittest.TestCase):

    def test_detects_429(self):
        self.assertTrue(is_rate_limited(rate_limited()))
        self.assertTrue(
            is_rate_limited(Exception("429 Client Error: Too Many Requests for url"))
        )
        self.assertFalse(is_rate_limited(FakeHTTPError(404)))
        self.assertFalse(is_rate_limited(ValueError("boom")))

    def test_retries_then_succeeds(self):
        fn = mock.Mock(side_effect=[rate_limited(), rate_limited(), "ok"])
        sleep = mock.Mock()
        result = retry_on_429(
            fn, "test", attempts=5, base_delay=10, max_delay=1000, sleep=sleep
        )
        self.assertEqual(result, "ok")
        self.assertEqual(fn.call_count, 3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [10, 20])

    def test_gives_up_and_raises_the_429(self):
        err = rate_limited()
        fn = mock.Mock(side_effect=err)
        sleep = mock.Mock()
        with self.assertRaises(FakeHTTPError) as ctx:
            retry_on_429(fn, "test", attempts=3, base_delay=1, sleep=sleep)
        self.assertIs(ctx.exception, err)
        self.assertEqual(fn.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_other_errors_not_retried(self):
        fn = mock.Mock(side_effect=FakeHTTPError(500))
        sleep = mock.Mock()
        with self.assertRaises(FakeHTTPError):
            retry_on_429(fn, "test", attempts=5, sleep=sleep)
        self.assertEqual(fn.call_count, 1)
        sleep.assert_not_called()

    def test_honours_retry_after_and_caps_delay(self):
        fn = mock.Mock(
            side_effect=[
                rate_limited({"Retry-After": "7"}),
                rate_limited({"Retry-After": "9999"}),
                "ok",
            ]
        )
        sleep = mock.Mock()
        retry_on_429(fn, "test", attempts=5, base_delay=30, max_delay=120, sleep=sleep)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [7, 120])


if __name__ == "__main__":
    unittest.main()
