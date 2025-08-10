import unittest
import os
import shutil
import time
import pytest
import tempfile
from datetime import datetime
from remotes.long_term.file_storage import DummyFileStorage
from remotes.long_term.kaggle import KaggleUploader


def long_term_test_case(
    long_term_db_target,
    dataset_path=None,
    dataset_remote_name=None,
    when=datetime.now(),
    **kwargs
):

    class TestLongTermDatabaseUploader(unittest.TestCase):

        def setUp(self):
            # Create temporary directories for testing
            self.temp_dir = tempfile.TemporaryDirectory()
            self.dataset_path = os.path.join(
                self.temp_dir.name, dataset_path or "dataset-path"
            )
            self.dataset_remote_name = dataset_remote_name or "test-super-dataset"

            self.uploader = long_term_db_target(
                self.dataset_path, self.dataset_remote_name, when, **kwargs
            )

        def test_increase_index(self):
            initial_index = self.uploader.get_current_index()
            self.uploader.increase_index()
            self.assertEqual(self.uploader.get_current_index(), initial_index + 1)

        def test_work_with_remote_dataset(self):
            os.makedirs(self.dataset_path, exist_ok=True)
            test_file_path = os.path.join(self.temp_dir.name, "test.txt")
            with open(test_file_path, "w") as f:
                f.write("test")

            self.uploader.stage(test_file_path)
            self.assertFalse(self.uploader.was_updated_in_last(seconds=1))
            self.uploader.upload_to_dataset("test_message", **{"test": "test"})
            time.sleep(3)  # kaggle need a moment
            self.assertTrue(self.uploader.was_updated_in_last(seconds=120))
            files = self.uploader.list_files()
            self.assertEqual(len(files), 1)
            self.assertEqual(os.path.basename(files[0]), "test.txt")

            file_content = self.uploader.get_file_content("test.txt")
            self.assertEqual(file_content, "test")

        def tearDown(self):
            # Clean up using the temporary directory manager
            self.temp_dir.cleanup()
            super().tearDown()

    return TestLongTermDatabaseUploader


class DummyLongTermTest(long_term_test_case(DummyFileStorage)):
    pass


class KaggleLongTermTest(long_term_test_case(KaggleUploader)):
    pass


if __name__ == "__main__":
    unittest.main()
