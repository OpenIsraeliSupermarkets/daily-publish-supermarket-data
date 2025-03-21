import unittest
import os
import shutil
from datetime import datetime, timedelta
from remotes.long_term.file_storage import DummyFileStorage
from remotes.long_term.kaggle import KaggleUploader
from unittest.mock import MagicMock


def long_term_test_case(long_term_db_target, **kwargs):

    class TestLongTermDatabaseUploader(unittest.TestCase):
        def setUp(self):
            self.uploader = long_term_db_target(**kwargs)

        def test_increase_index(self):
            initial_index = self.uploader.get_current_index()
            self.uploader.increase_index()
            self.assertEqual(self.uploader.get_current_index(), initial_index + 1)

        def test_upload_to_dataset(self):
            test_message = "Test upload"
            self.assertFalse(self.uploader.was_updated_in_last())
            self.uploader.upload_to_dataset(test_message)
            self.assertTrue(self.uploader.was_updated_in_last())

    return TestLongTermDatabaseUploader


class DummyLongTermTest(long_term_test_case(DummyFileStorage)):

    def tearDown(self):
        # Clean up the database file after each test
        if os.path.exists("israeli-supermarkets-2024"):
            shutil.rmtree("israeli-supermarkets-2024")
        super().tearDown()


class KaggleLongTermTest(
    long_term_test_case(KaggleUploader, dataset_remote_name="test-super-dataset")
):
    pass


if __name__ == "__main__":
    unittest.main()
