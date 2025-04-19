import unittest
import os
import shutil
import time
from datetime import datetime, timedelta
from remotes.long_term.file_storage import DummyFileStorage
from remotes.long_term.kaggle import KaggleUploader
from unittest.mock import MagicMock
from il_supermarket_scarper import ScraperFactory

def long_term_test_case(
    long_term_db_target,
    dataset_path="test-path",
    dataset_remote_name="test-super-dataset",
    when=datetime.now(),
    **kwargs
):

    class TestLongTermDatabaseUploader(unittest.TestCase):

        def setUp(self):
            self.uploader = long_term_db_target(
                dataset_path, dataset_remote_name, when, **kwargs
            )
            self.dataset_path = dataset_path
            self.dataset_remote_name = dataset_remote_name

        def test_increase_index(self):
            initial_index = self.uploader.get_current_index()
            self.uploader.increase_index()
            self.assertEqual(self.uploader.get_current_index(), initial_index + 1)

        def test_work_with_remote_dataset(self):
            os.makedirs(self.dataset_path, exist_ok=True)
            with open("test.txt", "w") as f:
                f.write("test")
                
            self.uploader.stage("test.txt")
            self.assertFalse(self.uploader.was_updated_in_last(seconds=1))
            self.uploader.upload_to_dataset("test_message", **{"test": "test"})
            time.sleep(3)  # kaggle need a momnet
            self.assertTrue(self.uploader.was_updated_in_last(seconds=120))
            files = self.uploader.list_files()
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0], "test.txt")
            
            file_content = self.uploader.get_file_content("test.txt")
            self.assertEqual(file_content, "test")

            
            
            

        def tearDown(self):
            # Clean up the database file after each test
            if os.path.exists(dataset_remote_name):
                shutil.rmtree(dataset_remote_name)

            if os.path.exists(dataset_path):
                shutil.rmtree(dataset_path)
            super().tearDown()
    return TestLongTermDatabaseUploader


class DummyLongTermTest(long_term_test_case(DummyFileStorage)):

    def tearDown(self):
        # Clean up the database file after each test
        if os.path.exists("test-path"):
            shutil.rmtree("test-path")

        super().tearDown()


class KaggleLongTermTest(long_term_test_case(KaggleUploader)):
    pass


if __name__ == "__main__":
    unittest.main()
