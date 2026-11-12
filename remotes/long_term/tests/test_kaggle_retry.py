"""Retry-on-429 for KaggleUploader (#167). No network, no real sleeping."""

import os
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

from remotes.long_term import kaggle as kaggle_module
from remotes.long_term.kaggle import (
    KaggleUploader,
    is_rate_limited,
    retry_on_rate_limit,
)


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
        result = retry_on_rate_limit(
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
            retry_on_rate_limit(fn, "test", attempts=3, base_delay=1, sleep=sleep)
        self.assertIs(ctx.exception, err)
        self.assertEqual(fn.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_other_errors_not_retried(self):
        fn = mock.Mock(side_effect=FakeHTTPError(500))
        sleep = mock.Mock()
        with self.assertRaises(FakeHTTPError):
            retry_on_rate_limit(fn, "test", attempts=5, sleep=sleep)
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
        retry_on_rate_limit(
            fn, "test", attempts=5, base_delay=30, max_delay=120, sleep=sleep
        )
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [7, 120])


class TestKaggleUploaderRetries(unittest.TestCase):
    """Every kagglehub call in KaggleUploader goes through the retry."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.kagglehub = mock.Mock()
        patches = [
            mock.patch.object(kaggle_module, "KAGGLEHUB_AVAILABLE", None),
            mock.patch.object(kaggle_module, "kagglehub", self.kagglehub, create=True),
            mock.patch.object(kaggle_module.time, "sleep"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.uploader = KaggleUploader(
            "owner/ds",
            datetime.now(),
            dataset_path=os.path.join(self.tmp.name, "ds"),
        )

    def _remote_file(self, name, content):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def test_index_download_retries_429(self):
        index_file = self._remote_file("index.json", "{}")
        self.kagglehub.dataset_download.side_effect = [rate_limited(), index_file]
        self.assertEqual(self.uploader._sync_n_load_index(), {})
        self.assertEqual(self.kagglehub.dataset_download.call_count, 2)

    def test_index_404_still_returns_none(self):
        self.kagglehub.dataset_download.side_effect = FakeHTTPError(404)
        self.assertIsNone(self.uploader._sync_n_load_index())
        self.assertEqual(self.kagglehub.dataset_download.call_count, 1)

    def test_get_file_content_retries_429(self):
        path = self._remote_file("a.json", '{"x": 1}')
        self.kagglehub.dataset_download.side_effect = [rate_limited(), path]
        self.assertEqual(self.uploader.get_file_content("a.json"), {"x": 1})

    def test_fetch_file_retries_429(self):
        path = self._remote_file("b.txt", "hi")
        self.kagglehub.dataset_download.side_effect = [rate_limited(), path]
        dest = os.path.join(self.tmp.name, "dest")
        out = self.uploader.fetch_file("b.txt", dest)
        self.assertTrue(os.path.exists(out))

    def test_download_retries_429(self):
        src = os.path.join(self.tmp.name, "remote")
        os.makedirs(src)
        self.kagglehub.dataset_download.side_effect = [rate_limited(), src]
        self.assertEqual(self.uploader.download(), self.uploader.dataset_path)

    def test_upload_retries_429(self):
        self.kagglehub.dataset_upload.side_effect = [rate_limited(), None]
        self.uploader.upload_to_dataset("notes")
        self.assertEqual(self.kagglehub.dataset_upload.call_count, 2)

    def test_upload_gives_up_after_attempts(self):
        self.kagglehub.dataset_upload.side_effect = rate_limited()
        with self.assertRaises(FakeHTTPError):
            self.uploader.upload_to_dataset("notes")
        self.assertEqual(
            self.kagglehub.dataset_upload.call_count,
            kaggle_module.KAGGLE_RETRY_ATTEMPTS,
        )


if __name__ == "__main__":
    unittest.main()
