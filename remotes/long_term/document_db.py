"""Module providing a file-system based implementation of a document database.

This module implements a simple document database using the file system,
primarily used for testing and development purposes.
"""

import os
import json
from ..utils import was_updated_within_hours
from .base import RemoteDatabaseUploader


class DummyDocumentDbUploader(RemoteDatabaseUploader):
    """A simple document database using the file system.

    This class implements a basic document database that stores data in JSON files
    on the local file system. It's primarily used for testing and development.
    """

    def __init__(self, db_path="db"):
        """Initialize the document database.

        Args:
            db_path (str): Path to the database directory
        """
        self.db_path = db_path
        self.tables_ids = self._load_tables_ids()

    def _load_tables_ids(self):
        """Load table IDs from the metadata file.

        Returns:
            dict: Dictionary of table IDs
        """
        meta_data_path = os.path.join(self.db_path, "meta_data.json")
        if not os.path.exists(meta_data_path):
            return {}
        with open(meta_data_path, encoding="utf-8") as f:
            return json.load(f)

    def _save_tables_ids(self):
        """Save table IDs to the metadata file."""
        meta_data_path = os.path.join(self.db_path, "meta_data.json")
        with open(meta_data_path, "w", encoding="utf-8") as f:
            json.dump(self.tables_ids, f)

    def _clean_meta_data(self):
        """Remove the metadata file."""
        meta_data_path = os.path.join(self.db_path, "meta_data.json")
        if os.path.exists(meta_data_path):
            os.remove(meta_data_path)

    def _insert_to_database(self, table_name, data):
        """Insert data into a table.

        Args:
            table_name (str): Name of the table
            data (list): List of records to insert
        """
        table_path = os.path.join(self.db_path, table_name)
        if not os.path.exists(table_path):
            os.makedirs(table_path)

        for record in data:
            record_id = self.tables_ids.get(table_name, 0)
            self.tables_ids[table_name] = record_id + 1
            file_path = os.path.join(table_path, f"{record_id}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(record, f)

        self._save_tables_ids()

    def _create_table(self, table_name):
        """Create a new table.

        Args:
            table_name (str): Name of the table to create
        """
        table_path = os.path.join(self.db_path, table_name)
        if not os.path.exists(table_path):
            os.makedirs(table_path)

    def _clean_all_tables(self):
        """Remove all tables and their data."""
        if os.path.exists(self.db_path):
            for table_name in os.listdir(self.db_path):
                table_path = os.path.join(self.db_path, table_name)
                if os.path.isdir(table_path):
                    for file_name in os.listdir(table_path):
                        file_path = os.path.join(table_path, file_name)
                        os.remove(file_path)
                    os.rmdir(table_path)
        self._clean_meta_data()

    def _get_all_files_by_chain(self, chain_id):
        """Get all files for a specific chain.

        Args:
            chain_id (str): ID of the chain to retrieve files for

        Returns:
            list: List of file paths for the chain
        """
        chain_path = os.path.join(self.db_path, chain_id)
        if not os.path.exists(chain_path):
            return []
        return [
            os.path.join(chain_path, f)
            for f in os.listdir(chain_path)
            if os.path.isfile(os.path.join(chain_path, f))
        ]

    def _get_content_of_file(self, file_path):
        """Read the content of a file.

        Args:
            file_path (str): Path to the file to read

        Returns:
            dict: Content of the file as a dictionary
        """
        with open(file_path, encoding="utf-8") as f:
            return json.load(f)

    def count_updated_cycles(self):
        """Count the number of updated cycles.

        Returns:
            int: Number of updated cycles
        """
        return len(os.listdir(self.db_path))

    def is_parser_updated(self) -> bool:
        """Check if any files in the database were updated in the last 24 hours.

        Returns:
            bool: True if any file was updated within last 24 hours, False otherwise
        """
        return was_updated_within_hours(self.db_path, 24)

    def was_updated_in_last_24h(self, hours: int = 24) -> bool:
        """Check if any files in the database were updated within specified hours.

        Args:
            hours (int, optional): Number of hours to look back. Defaults to 24.

        Returns:
            bool: True if any file was updated within specified hours, False otherwise
        """
        return was_updated_within_hours(self.db_path, hours)
