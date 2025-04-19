import unittest
from datetime import datetime, timedelta
from remotes.short_term.api_base import ShortTermDatabaseUploader
from remotes.short_term.mongo_db import MongoDbUploader
from remotes.short_term.file_db import DummyDocumentDbUploader
from unittest.mock import patch, MagicMock
import mongomock
import os
import shutil
import copy


def short_term_test_case(short_term_db_target):

    class TestShortTermDatabaseUploader(unittest.TestCase):
        def setUp(self):
            self.uploader: ShortTermDatabaseUploader = short_term_db_target

        def test_create_and_insert_to_table(self):
            # Test table creation
            self.uploader._create_table("id", "test_table")
            self.assertIn("test_table", self.uploader._list_tables())

            # Test data insertion
            test_items = [{"id": 1, "data": "test1"}, {"id": 2, "data": "test2"}]
            self.uploader._insert_to_database("test_table", copy.deepcopy(test_items))
            self.assertEqual(
                list(self.uploader._get_table_content("test_table")), test_items
            )

        def test_clean_all_tables(self):
            # Create some test tables
            self.uploader._create_table("id", "table1")
            self.uploader._create_table("id", "table2")

            # Clean all tables
            self.uploader._clean_all_tables()
            self.assertEqual(len(self.uploader._list_tables()), 0)

        def test_get_table_content(self):
            # Create test data
            self.uploader._create_table("id", "files")
            test_items = [
                {"id": "1", "chain": "chain1", "file_type": "csv", "data": "test1"},
                {"id": "2", "chain": "chain1", "file_type": "json", "data": "test2"},
                {"id": "3", "chain": "chain2", "file_type": "csv", "data": "test3"},
            ]
            self.uploader._insert_to_database("files", test_items)

            # Test filtering by chain
            chain1_files = self.uploader._get_table_content("files")
            self.assertEqual(len(chain1_files), 3)

            # Test filtering by chain and file type
            chain1_csv = self.uploader._get_table_content("files", {"file_type": "csv"})
            self.assertEqual(len(chain1_csv), 2)

        def test_collection_updated(self):
            # Test recent update
            self.uploader._create_table("id", "test_table")
            self.assertTrue(not self.uploader._is_collection_updated("test_table"))

            # Test old update
            self.uploader._insert_to_database(
                "test_table", [{"id": "1", "data": "test"}]
            )
            self.assertTrue(self.uploader._is_collection_updated("test_table"))

    return TestShortTermDatabaseUploader


class MongoTestCase(short_term_test_case(MongoDbUploader)):
    @patch("pymongo.MongoClient", mongomock.MongoClient)
    def setUp(self):
        super().setUp()


class DummyTestCase(short_term_test_case(DummyDocumentDbUploader(db_path="./document_db"))):

    def tearDown(self):
        # Clean up the database file after each test
        self.uploader._clean_all_tables()
        if os.path.exists("./document_db"):
            shutil.rmtree("./document_db")
        super().tearDown()


if __name__ == "__main__":
    unittest.main()
