import unittest
import os
import shutil
import time
from datetime import datetime, timedelta
from remotes.long_term.file_storage import DummyFileStorage
from remotes.long_term.kaggle import KaggleUploader
from unittest.mock import MagicMock


def long_term_test_case(long_term_db_target, dataset_path="test-path" ,dataset_remote_name="test-super-dataset", when=datetime.now(), **kwargs):

    class TestLongTermDatabaseUploader(unittest.TestCase):
        
        def setUp(self):
            self.uploader = long_term_db_target(dataset_path, dataset_remote_name, when, **kwargs)
            self.dataset_path = dataset_path
            self.dataset_remote_name = dataset_remote_name

        def test_increase_index(self):
            initial_index = self.uploader.get_current_index()
            self.uploader.increase_index()
            self.assertEqual(self.uploader.get_current_index(), initial_index + 1)

        def test_upload_to_dataset(self):
            os.makedirs(self.dataset_path, exist_ok=True)
            with open(os.path.join(self.dataset_path, "test.txt"), "w") as f:
                f.write("test")
            self.assertFalse(self.uploader.was_updated_in_last(seconds=1))
            self.uploader.upload_to_dataset("test_message", **{"test": "test"})
            time.sleep(1) # kaggle need a momnet
            self.assertTrue(self.uploader.was_updated_in_last(seconds=120))

    return TestLongTermDatabaseUploader


class DummyLongTermTest(long_term_test_case(DummyFileStorage)):

    def tearDown(self):
        # Clean up the database file after each test
        if os.path.exists("test-path"):
            shutil.rmtree("test-path")
            
        super().tearDown()


class KaggleLongTermTest(
    long_term_test_case(KaggleUploader)
):
    pass


if __name__ == "__main__":
    unittest.main()
